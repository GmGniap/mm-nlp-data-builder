#!/usr/bin/env python3
"""
Cold Storage Archival Pipeline (S3 + Parquet)
==============================================
Archives historical data older than 30 days from Neon PostgreSQL to AWS S3
in Snappy-compressed Apache Parquet format. Safely purges archived records from
the database while preserving all user annotations and active data.

Usage:
  # Dry-run: preview records eligible for archival
  python services/telegram_scraper/archival.py --dry-run

  # Archive both telegram_messages and clean_tele_text (default: >30 days)
  python services/telegram_scraper/archival.py

  # Archive raw messages only
  python services/telegram_scraper/archival.py --table telegram_messages

  # Archive with custom retention days
  python services/telegram_scraper/archival.py --retention-days 60
"""
import os
import sys
import io
import argparse
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add shared module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from services.telegram_scraper.storage import StorageHandler
from services.telegram_scraper.scraper import load_config, resolve_environment_and_schema

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("archival")


def get_s3_client(region: str = "ap-southeast-1"):
    """Initialize boto3 S3 client using environment or default AWS configuration."""
    import boto3
    from botocore.config import Config

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", region)

    cfg = Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"})

    if aws_access_key and aws_secret_key:
        return boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region,
            config=cfg
        )
    return boto3.client("s3", region_name=aws_region, config=cfg)


def build_s3_key(environment: str, table_name: str, channel_name: str,
                 category: str, record_date: datetime) -> str:
    """
    Format: <env>/<year>/<month>/<category>/<channel>/<table_name>-archived-<YYYY-MM-DD>.parquet
    """
    year = record_date.strftime("%Y")
    month = record_date.strftime("%m")
    date_str = record_date.strftime("%Y-%m-%d")
    clean_channel = (channel_name or "unknown").lstrip("@").replace("/", "_")
    cat = (category or "uncategorized").lower()

    return f"{environment}/{year}/{month}/{cat}/{clean_channel}/{table_name}-archived-{date_str}.parquet"


def records_to_parquet_buffer(records: list[dict], compression: str = "snappy") -> bytes:
    """Convert list of record dicts to compressed Parquet in-memory bytes using PyArrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(records)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression=compression)
    return sink.getvalue()


def verify_s3_parquet_row_count(s3_client, bucket: str, s3_key: str, expected_count: int) -> bool:
    """
    Download or read S3 Parquet object and verify row count matches expected database records.
    """
    import pyarrow.parquet as pq

    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    body = response["Body"].read()
    reader = pq.ParquetFile(io.BytesIO(body))
    actual_rows = reader.metadata.num_rows

    if actual_rows == expected_count:
        logger.info(f"  ✔ Parquet row count verified: {actual_rows} == {expected_count}")
        return True
    else:
        logger.error(f"  ❌ Row count mismatch for {s3_key}: S3 has {actual_rows}, expected {expected_count}")
        return False


def archive_raw_messages(storage: StorageHandler, s3_client, bucket: str, environment: str,
                         cutoff_date: datetime, dry_run: bool = False, chunk_size: int = 5000,
                         compression: str = "snappy") -> dict:
    """Archive eligible rows from telegram_messages table."""
    logger.info(f"Scanning 'telegram_messages' older than {cutoff_date.isoformat()} ...")
    raw_records = storage.get_unarchived_raw_messages(cutoff_date, limit=chunk_size)

    if not raw_records:
        logger.info("  No raw messages eligible for archival.")
        return {"scanned": 0, "archived": 0, "deleted": 0}

    logger.info(f"  Found {len(raw_records)} messages eligible for archival.")
    if dry_run:
        logger.info(f"  [DRY-RUN] Would archive {len(raw_records)} raw messages to S3.")
        return {"scanned": len(raw_records), "archived": len(raw_records), "deleted": 0}

    # Group records by (channel_name, date)
    groups = {}
    for r in raw_records:
        day_key = r.date.strftime("%Y-%m-%d") if r.date else "nodate"
        key = (r.channel_name, day_key)
        if key not in groups:
            groups[key] = []
        groups[key].append(r)

    total_archived = 0
    total_deleted = 0

    for (channel, day_str), recs in groups.items():
        sample_date = recs[0].date if recs[0].date else cutoff_date
        rec_cat = getattr(recs[0], "category", None) or "general"
        s3_key = build_s3_key(environment, "telegram_messages", channel, rec_cat, sample_date)
        s3_uri = f"s3://{bucket}/{s3_key}"

        dict_list = [
            {
                "id": r.id,
                "channel_name": r.channel_name,
                "category": getattr(r, "category", None),
                "message_id": r.message_id,
                "message_text": r.message_text,
                "date": r.date.isoformat() if r.date else None,
                "media_url": r.media_url,
                "status": r.status,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }
            for r in recs
        ]

        log_row = storage.start_archival_log(
            table_name="telegram_messages",
            cutoff_date=cutoff_date,
            s3_uri=s3_uri,
            channel_name=channel,
            category=rec_cat
        )

        try:
            parquet_bytes = records_to_parquet_buffer(dict_list, compression=compression)
            file_size = len(parquet_bytes)

            logger.info(f"  Uploading {len(recs)} records to {s3_uri} ({file_size} bytes)...")
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream"
            )

            # Verification check
            if verify_s3_parquet_row_count(s3_client, bucket, s3_key, len(recs)):
                # Safe deletion
                id_list = [r.id for r in recs]
                deleted_count = storage.delete_raw_messages_by_ids(id_list)
                storage.finish_archival_log(log_row.id, len(recs), file_size, status="completed")
                total_archived += len(recs)
                total_deleted += deleted_count
                logger.info(f"  ✔ Successfully archived & deleted {deleted_count} messages for {channel}")
            else:
                storage.finish_archival_log(log_row.id, 0, file_size, status="failed",
                                           error_message="S3 row count verification failed")
        except Exception as e:
            logger.error(f"  ❌ Archival failed for {s3_uri}: {e}")
            storage.finish_archival_log(log_row.id, 0, 0, status="failed", error_message=str(e))

    return {"scanned": len(raw_records), "archived": total_archived, "deleted": total_deleted}


def archive_clean_text(storage: StorageHandler, s3_client, bucket: str, environment: str,
                       cutoff_date: datetime, dry_run: bool = False, chunk_size: int = 5000,
                       compression: str = "snappy") -> dict:
    """
    Archive eligible rows from clean_tele_text table.
    Strictly safeguards human annotations: rows referenced in annotation_results are excluded.
    """
    logger.info(f"Scanning 'clean_tele_text' older than {cutoff_date.isoformat()} (excluding active annotations)...")
    clean_records = storage.get_unarchived_clean_text_safe(cutoff_date, limit=chunk_size)

    if not clean_records:
        logger.info("  No clean text records eligible for archival.")
        return {"scanned": 0, "archived": 0, "deleted": 0}

    logger.info(f"  Found {len(clean_records)} unannotated clean sentences eligible for archival.")
    if dry_run:
        logger.info(f"  [DRY-RUN] Would archive {len(clean_records)} clean sentences to S3.")
        return {"scanned": len(clean_records), "archived": len(clean_records), "deleted": 0}

    # Group by (channel_name, date)
    groups = {}
    for r in clean_records:
        day_key = r.created_at.strftime("%Y-%m-%d") if r.created_at else "nodate"
        key = (r.channel_name, day_key)
        if key not in groups:
            groups[key] = []
        groups[key].append(r)

    total_archived = 0
    total_deleted = 0

    for (channel, day_str), recs in groups.items():
        sample_date = recs[0].created_at if recs[0].created_at else cutoff_date
        s3_key = build_s3_key(environment, "clean_tele_text", channel, "general", sample_date)
        s3_uri = f"s3://{bucket}/{s3_key}"

        dict_list = [
            {
                "id": r.id,
                "telegram_message_id": r.telegram_message_id,
                "line_index": r.line_index,
                "sentence": r.sentence,
                "channel_name": r.channel_name,
                "source_message_id": r.source_message_id,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }
            for r in recs
        ]

        log_row = storage.start_archival_log(
            table_name="clean_tele_text",
            cutoff_date=cutoff_date,
            s3_uri=s3_uri,
            channel_name=channel
        )

        try:
            parquet_bytes = records_to_parquet_buffer(dict_list, compression=compression)
            file_size = len(parquet_bytes)

            logger.info(f"  Uploading {len(recs)} clean records to {s3_uri} ({file_size} bytes)...")
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream"
            )

            # Verification check
            if verify_s3_parquet_row_count(s3_client, bucket, s3_key, len(recs)):
                id_list = [r.id for r in recs]
                # Cascades to delete associated skipped_records without losing annotation_results
                deleted_count = storage.delete_clean_text_by_ids(id_list)
                storage.finish_archival_log(log_row.id, len(recs), file_size, status="completed")
                total_archived += len(recs)
                total_deleted += deleted_count
                logger.info(f"  ✔ Successfully archived & purged {deleted_count} clean text rows for {channel}")
            else:
                storage.finish_archival_log(log_row.id, 0, file_size, status="failed",
                                           error_message="S3 row count verification failed")
        except Exception as e:
            logger.error(f"  ❌ Clean text archival failed for {s3_uri}: {e}")
            storage.finish_archival_log(log_row.id, 0, 0, status="failed", error_message=str(e))

    return {"scanned": len(clean_records), "archived": total_archived, "deleted": total_deleted}


def run_archival(table: str = "all", retention_days: int = 30, dry_run: bool = False,
                 config_file: str = None, env: str = None):
    config = load_config(config_file)
    environment, schema = resolve_environment_and_schema(config, env_override=env)

    archival_cfg = config.get("archival", {})
    bucket = os.getenv("S3_ARCHIVE_BUCKET_NAME") or archival_cfg.get("s3", {}).get("bucket_name", "myanmar-nlp-data-archive")
    region = os.getenv("AWS_REGION") or archival_cfg.get("s3", {}).get("region", "ap-southeast-1")
    compression = archival_cfg.get("compression", "snappy")
    chunk_size = int(archival_cfg.get("batch_chunk_size", 5000))
    safety_offset = int(archival_cfg.get("scan_safety_offset_days", 2))

    now_utc = datetime.now(timezone.utc)
    # Cutoff threshold older than retention_days (e.g. 30 days)
    cutoff_date = now_utc - timedelta(days=retention_days)

    logger.info("=" * 65)
    logger.info("  📦 Telegram Scraper Cold Storage Archival Pipeline")
    logger.info("=" * 65)
    logger.info(f"  Environment    : {environment} (schema: {schema})")
    logger.info(f"  Table Target   : {table}")
    logger.info(f"  Retention Days : {retention_days} (Cutoff: <= {cutoff_date.strftime('%Y-%m-%d %H:%M:%S UTC')})")
    logger.info(f"  S3 Bucket      : {bucket} (Region: {region})")
    logger.info(f"  Compression    : {compression}")
    logger.info(f"  Dry Run        : {dry_run}")
    logger.info("=" * 65)

    pg_url = os.getenv("NEON_DATABASE_URL") or config.get("postgresql", {}).get("url", "")
    storage = StorageHandler(pg_url=pg_url, schema=schema)

    s3_client = None
    if not dry_run:
        try:
            s3_client = get_s3_client(region=region)
        except Exception as err:
            logger.error(f"Failed to initialize S3 client: {err}")
            storage.close()
            return

    results = {}
    if table in ("all", "telegram_messages"):
        results["telegram_messages"] = archive_raw_messages(
            storage=storage,
            s3_client=s3_client,
            bucket=bucket,
            environment=environment,
            cutoff_date=cutoff_date,
            dry_run=dry_run,
            chunk_size=chunk_size,
            compression=compression
        )

    if table in ("all", "clean_tele_text"):
        results["clean_tele_text"] = archive_clean_text(
            storage=storage,
            s3_client=s3_client,
            bucket=bucket,
            environment=environment,
            cutoff_date=cutoff_date,
            dry_run=dry_run,
            chunk_size=chunk_size,
            compression=compression
        )

    storage.close()
    logger.info("=" * 65)
    logger.info(f"  Archival run completed. Summary: {results}")
    logger.info("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Archive historical data from Neon PostgreSQL to AWS S3 Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--table", choices=["all", "telegram_messages", "clean_tele_text"], default="all",
        help="Target table to archive (default: all)"
    )
    parser.add_argument(
        "--retention-days", type=int, default=30,
        help="Number of days of data to retain in PostgreSQL (default: 30)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview archival candidates without modifying S3 or PostgreSQL"
    )
    parser.add_argument(
        "--env", choices=["dev", "prod"], default=None,
        help="Target environment ('dev' or 'prod')"
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to custom config YAML file"
    )
    args = parser.parse_args()

    run_archival(
        table=args.table,
        retention_days=args.retention_days,
        dry_run=args.dry_run,
        config_file=args.config,
        env=args.env
    )
