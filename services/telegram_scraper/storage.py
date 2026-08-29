import os
import sys
from datetime import datetime, timezone
from sqlalchemy.orm import sessionmaker

# Add shared module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from shared.scraper_models import TelegramMessage, ScrapingLog, init_scraper_db


class StorageHandler:
    def __init__(self, pg_url: str):
        """
        Initialise connection to Neon PostgreSQL using the scraper-owned models.

        Parameters
        ----------
        pg_url : str
            Neon PostgreSQL connection string, e.g.
            "postgresql://user:pw@host/db?sslmode=require".
            Read from config key ``postgresql.url`` or env var NEON_DATABASE_URL.
        """
        self.engine = init_scraper_db(pg_url)
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
                           scrape_end_ts: datetime) -> ScrapingLog:
        """
        Upsert a ScrapingLog row for (channel_name, run_date) and mark it 'running'.
        If a previous failed row exists it is reused; otherwise a new row
        is created.
        """
        log = self.session.query(ScrapingLog).filter_by(
            channel_name=channel_name,
            run_date=run_date
        ).first()
        if log is None:
            log = ScrapingLog(
                channel_name=channel_name,
                run_date=run_date,
                scrape_start_ts=scrape_start_ts,
                scrape_end_ts=scrape_end_ts,
            )
            self.session.add(log)
        else:
            log.scrape_start_ts = scrape_start_ts
            log.scrape_end_ts   = scrape_end_ts
            log.run_started_at  = datetime.now(timezone.utc)

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

    def close(self):
        self.session.close()
