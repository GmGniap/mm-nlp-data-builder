import os
import sys
from datetime import datetime, timezone
from sqlalchemy.orm import sessionmaker

# Add shared module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared.scraper_models import (
    TelegramMessage, ScrapingLog, ScrapingErrorLog, ArchivalLog, init_scraper_db
)


class StorageHandler:
    def __init__(self, pg_url: str, schema: str = "public"):
        """
        Initialise connection to Neon PostgreSQL using the scraper-owned models.

        Parameters
        ----------
        pg_url : str
            Neon PostgreSQL connection string, e.g.
            "postgresql://user:pw@host/db?sslmode=require".
            Read from config key ``postgresql.url`` or env var NEON_DATABASE_URL.
        schema : str, optional
            PostgreSQL schema to use ('public' for dev, 'production' for prod).
            Default is 'public'.
        """
        self.schema = schema
        self.engine = init_scraper_db(pg_url, schema=schema)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    # -------------------------------------------------------------------------
    # Message persistence
    # -------------------------------------------------------------------------

    def save_messages(self, messages_data: list) -> tuple[int, int]:
        """
        Insert new TelegramMessage records, skip duplicates keyed on
        (channel_name, message_id).  Returns (saved, skipped).
        """
        saved_count   = 0
        skipped_count = 0

        for item in messages_data:
            existing = self.session.query(TelegramMessage).filter_by(
                channel_name=item['channel_name'],
                message_id=item['message_id']
            ).first()

            if existing:
                skipped_count += 1
                continue

            msg = TelegramMessage(
                channel_name=item['channel_name'],
                category=item.get('category'),
                message_id=item['message_id'],
                message_text=item['message_text'],
                date=item.get('date'),
                media_url=item.get('media_url'),
                status='pending'
            )
            self.session.add(msg)
            saved_count += 1

        self.session.commit()
        return saved_count, skipped_count

    def delete_messages_for_channel_window(self, channel_name: str,
                                            day_start: datetime,
                                            day_end: datetime) -> int:
        """
        Delete existing TelegramMessage records for a channel within [day_start, day_end].
        Used when --force is supplied. Returns number of rows deleted.
        """
        deleted = (
            self.session.query(TelegramMessage)
            .filter(
                TelegramMessage.channel_name == channel_name,
                TelegramMessage.date.isnot(None),
                TelegramMessage.date >= day_start,
                TelegramMessage.date <= day_end,
            )
            .delete(synchronize_session=False)
        )
        self.session.commit()
        return deleted

    # -------------------------------------------------------------------------
    # Watermark / ScrapingLog helpers
    # -------------------------------------------------------------------------

    def channel_day_already_scraped(self, channel_name: str, run_date: str) -> bool:
        """
        Return True if a 'completed' ScrapingLog row already exists for
        the given channel and run_date (format: 'YYYY-MM-DD').  Idempotency guard.
        """
        log = self.session.query(ScrapingLog).filter_by(
            channel_name=channel_name,
            run_date=run_date,
            status='completed'
        ).first()
        return log is not None

    def start_scraping_log(self, channel_name: str, run_date: str,
                           scrape_start_ts: datetime,
                           scrape_end_ts: datetime,
                           category: str = None,
                           force: bool = False) -> ScrapingLog:
        """
        Upsert a ScrapingLog row for (channel_name, run_date) and mark it 'running'.
        If force=True, any existing row is reset.
        """
        log = self.session.query(ScrapingLog).filter_by(
            channel_name=channel_name,
            run_date=run_date
        ).first()
        if log is None:
            log = ScrapingLog(
                channel_name=channel_name,
                category=category,
                run_date=run_date,
                scrape_start_ts=scrape_start_ts,
                scrape_end_ts=scrape_end_ts,
            )
            self.session.add(log)
        else:
            if category:
                log.category = category
            log.scrape_start_ts = scrape_start_ts
            log.scrape_end_ts   = scrape_end_ts
            log.run_started_at  = datetime.now(timezone.utc)
            log.messages_scraped = 0
            log.messages_saved   = 0
            log.messages_skipped = 0

        log.status          = 'running'
        log.run_finished_at = None
        self.session.commit()
        return log

    def finish_scraping_log(self, log: ScrapingLog,
                            messages_scraped: int,
                            messages_saved: int,
                            messages_skipped: int,
                            status: str = 'completed') -> None:
        """Finalise a ScrapingLog row with result counts and mark it completed."""
        log.messages_scraped  = messages_scraped
        log.messages_saved    = messages_saved
        log.messages_skipped  = messages_skipped
        log.run_finished_at   = datetime.now(timezone.utc)
        log.status            = status
        self.session.commit()

    def get_scraping_logs(self, limit: int = 30) -> list[ScrapingLog]:
        """Return recent scraping log rows, newest first."""
        return (
            self.session.query(ScrapingLog)
            .order_by(ScrapingLog.run_date.desc())
            .limit(limit)
            .all()
        )

    # -------------------------------------------------------------------------
    # DLQ (ScrapingErrorLog) Helpers
    # -------------------------------------------------------------------------

    def log_scraping_error(self, category: str, channel_name: str, run_date: str,
                           error_type: str, error_message: str,
                           stack_trace: str = None, retry_count: int = 0) -> ScrapingErrorLog:
        """Record channel scraping failure to scraping_error_logs DLQ table."""
        err_log = ScrapingErrorLog(
            category=category,
            channel_name=str(channel_name),
            run_date=run_date,
            error_type=str(error_type),
            error_message=str(error_message),
            stack_trace=stack_trace,
            retry_count=retry_count,
            resolved=False,
            created_at=datetime.now(timezone.utc)
        )
        self.session.add(err_log)
        self.session.commit()
        return err_log

    # -------------------------------------------------------------------------
    # Archival & Cold Storage Helpers
    # -------------------------------------------------------------------------

    def get_unarchived_raw_messages(self, cutoff_date: datetime, limit: int = 5000) -> list[TelegramMessage]:
        """Fetch raw messages older than cutoff_date."""
        return (
            self.session.query(TelegramMessage)
            .filter(
                TelegramMessage.date.isnot(None),
                TelegramMessage.date < cutoff_date
            )
            .order_by(TelegramMessage.date.asc())
            .limit(limit)
            .all()
        )

    def get_unarchived_clean_text_safe(self, cutoff_date: datetime, limit: int = 5000):
        """
        Fetch clean text records older than cutoff_date that do NOT have
        active user annotations. Rows associated with skipped_records can be purged.
        """
        from shared.annotation_models import CleanTeleText, AnnotationResult
        annotated_ids_subquery = self.session.query(AnnotationResult.clean_line_id).distinct()
        return (
            self.session.query(CleanTeleText)
            .filter(
                CleanTeleText.created_at < cutoff_date,
                ~CleanTeleText.id.in_(annotated_ids_subquery)
            )
            .order_by(CleanTeleText.created_at.asc())
            .limit(limit)
            .all()
        )

    def delete_raw_messages_by_ids(self, id_list: list[int]) -> int:
        """Safely delete raw messages by primary key list."""
        if not id_list:
            return 0
        deleted = (
            self.session.query(TelegramMessage)
            .filter(TelegramMessage.id.in_(id_list))
            .delete(synchronize_session=False)
        )
        self.session.commit()
        return deleted

    def delete_clean_text_by_ids(self, id_list: list[int]) -> int:
        """Safely delete clean text rows by primary key list, along with associated skipped_records."""
        if not id_list:
            return 0
        from shared.annotation_models import CleanTeleText, SkippedRecord
        # Explicitly delete associated skipped records from both tables
        self.session.query(SkippedRecord).filter(
            SkippedRecord.clean_line_id.in_(id_list)
        ).delete(synchronize_session=False)

        deleted = (
            self.session.query(CleanTeleText)
            .filter(CleanTeleText.id.in_(id_list))
            .delete(synchronize_session=False)
        )
        self.session.commit()
        return deleted


    def start_archival_log(self, table_name: str, cutoff_date: datetime, s3_uri: str,
                           category: str = None, channel_name: str = None) -> ArchivalLog:
        """Initialize an archival audit log entry."""
        log = ArchivalLog(
            table_name=table_name,
            category=category,
            channel_name=channel_name,
            cutoff_date=cutoff_date,
            s3_uri=s3_uri,
            status='in_progress',
            created_at=datetime.now(timezone.utc)
        )
        self.session.add(log)
        self.session.commit()
        return log

    def finish_archival_log(self, log_id: int, rows_archived: int, file_size_bytes: int,
                            status: str = 'completed', error_message: str = None) -> None:
        """Update an archival audit log entry upon completion or failure."""
        log = self.session.query(ArchivalLog).filter_by(id=log_id).first()
        if log:
            log.rows_archived = rows_archived
            log.file_size_bytes = file_size_bytes
            log.status = status
            log.error_message = error_message
            log.finished_at = datetime.now(timezone.utc)
            self.session.commit()

    # -------------------------------------------------------------------------

    def close(self):
        self.session.close()
