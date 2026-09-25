"""initial_schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-09-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0001_initial_schema'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Annotation platform tables
    op.create_table('clean_tele_extra_info',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=False),
        sa.Column('message_id', sa.BigInteger(), nullable=False),
        sa.Column('headline', sa.Text(), nullable=True),
        sa.Column('clean_info_date', sa.String(length=100), nullable=True),
        sa.Column('original_short_note', sa.Text(), nullable=True),
        sa.Column('url_lists', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_clean_tele_extra_info_category'), 'clean_tele_extra_info', ['category'], unique=False)
    op.create_index(op.f('ix_clean_tele_extra_info_channel_name'), 'clean_tele_extra_info', ['channel_name'], unique=False)
    op.create_index(op.f('ix_clean_tele_extra_info_message_id'), 'clean_tele_extra_info', ['message_id'], unique=False)

    op.create_table('clean_tele_text',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('telegram_message_id', sa.Integer(), nullable=True),
        sa.Column('line_index', sa.Integer(), nullable=False),
        sa.Column('sentence', sa.Text(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=True),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('source_message_id', sa.BigInteger(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_clean_tele_text_category'), 'clean_tele_text', ['category'], unique=False)
    op.create_index(op.f('ix_clean_tele_text_channel_name'), 'clean_tele_text', ['channel_name'], unique=False)
    op.create_index(op.f('ix_clean_tele_text_telegram_message_id'), 'clean_tele_text', ['telegram_message_id'], unique=False)

    op.create_table('cleaning_error_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=False),
        sa.Column('run_date', sa.String(length=10), nullable=False),
        sa.Column('telegram_message_id', sa.Integer(), nullable=True),
        sa.Column('source_message_id', sa.BigInteger(), nullable=True),
        sa.Column('raw_text', sa.Text(), nullable=True),
        sa.Column('error_type', sa.String(length=100), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=False),
        sa.Column('stack_trace', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=True),
        sa.Column('resolved', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cleaning_error_logs_category'), 'cleaning_error_logs', ['category'], unique=False)
    op.create_index(op.f('ix_cleaning_error_logs_channel_name'), 'cleaning_error_logs', ['channel_name'], unique=False)
    op.create_index(op.f('ix_cleaning_error_logs_run_date'), 'cleaning_error_logs', ['run_date'], unique=False)

    op.create_table('cleaning_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('run_date', sa.String(length=10), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('messages_processed', sa.Integer(), nullable=True),
        sa.Column('messages_skipped', sa.Integer(), nullable=True),
        sa.Column('sentences_generated', sa.Integer(), nullable=True),
        sa.Column('cleaning_start_ts', sa.DateTime(), nullable=False),
        sa.Column('cleaning_end_ts', sa.DateTime(), nullable=True),
        sa.Column('run_started_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('channel_name', 'run_date', name='uq_cleaning_channel_rundate')
    )
    op.create_index(op.f('ix_cleaning_logs_category'), 'cleaning_logs', ['category'], unique=False)
    op.create_index(op.f('ix_cleaning_logs_channel_name'), 'cleaning_logs', ['channel_name'], unique=False)
    op.create_index(op.f('ix_cleaning_logs_run_date'), 'cleaning_logs', ['run_date'], unique=False)

    op.create_table('users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(length=120), nullable=False),
        sa.Column('password_hash', sa.String(length=256), nullable=False),
        sa.Column('role', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('email')
    )

    op.create_table('annotation_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('clean_line_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('annotation_type', sa.String(length=50), nullable=False),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['clean_line_id'], ['clean_tele_text.id'], name='fk_annotation_results_clean_line_id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('clean_line_id', 'user_id', 'annotation_type', name='uq_annotation_result_line_user_type')
    )
    op.create_index(op.f('ix_annotation_results_annotation_type'), 'annotation_results', ['annotation_type'], unique=False)
    op.create_index(op.f('ix_annotation_results_clean_line_id'), 'annotation_results', ['clean_line_id'], unique=False)
    op.create_index(op.f('ix_annotation_results_user_id'), 'annotation_results', ['user_id'], unique=False)

    op.create_table('skipped_records',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('clean_line_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('annotation_type', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['clean_line_id'], ['clean_tele_text.id'], name='fk_skipped_records_clean_line_id'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('clean_line_id', 'user_id', 'annotation_type', name='uq_skipped_record_line_user_type')
    )
    op.create_index(op.f('ix_skipped_records_annotation_type'), 'skipped_records', ['annotation_type'], unique=False)
    op.create_index(op.f('ix_skipped_records_clean_line_id'), 'skipped_records', ['clean_line_id'], unique=False)
    op.create_index(op.f('ix_skipped_records_user_id'), 'skipped_records', ['user_id'], unique=False)

    # Scraper platform tables
    op.create_table('archival_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('table_name', sa.String(length=50), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=True),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('cutoff_date', sa.DateTime(), nullable=False),
        sa.Column('s3_uri', sa.String(length=500), nullable=False),
        sa.Column('rows_archived', sa.Integer(), nullable=True),
        sa.Column('file_size_bytes', sa.BigInteger(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_archival_logs_category'), 'archival_logs', ['category'], unique=False)
    op.create_index(op.f('ix_archival_logs_cutoff_date'), 'archival_logs', ['cutoff_date'], unique=False)
    op.create_index(op.f('ix_archival_logs_table_name'), 'archival_logs', ['table_name'], unique=False)

    op.create_table('scraping_error_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('run_date', sa.String(length=10), nullable=False),
        sa.Column('error_type', sa.String(length=100), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=False),
        sa.Column('stack_trace', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=True),
        sa.Column('resolved', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_scraping_error_logs_category'), 'scraping_error_logs', ['category'], unique=False)
    op.create_index(op.f('ix_scraping_error_logs_channel_name'), 'scraping_error_logs', ['channel_name'], unique=False)
    op.create_index(op.f('ix_scraping_error_logs_run_date'), 'scraping_error_logs', ['run_date'], unique=False)

    op.create_table('scraping_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('run_date', sa.String(length=10), nullable=False),
        sa.Column('messages_scraped', sa.Integer(), nullable=True),
        sa.Column('messages_saved', sa.Integer(), nullable=True),
        sa.Column('messages_skipped', sa.Integer(), nullable=True),
        sa.Column('scrape_start_ts', sa.DateTime(), nullable=False),
        sa.Column('scrape_end_ts', sa.DateTime(), nullable=False),
        sa.Column('run_started_at', sa.DateTime(), nullable=True),
        sa.Column('run_finished_at', sa.DateTime(), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('channel_name', 'run_date', name='uq_channel_rundate')
    )
    op.create_index(op.f('ix_scraping_logs_category'), 'scraping_logs', ['category'], unique=False)
    op.create_index(op.f('ix_scraping_logs_channel_name'), 'scraping_logs', ['channel_name'], unique=False)
    op.create_index(op.f('ix_scraping_logs_run_date'), 'scraping_logs', ['run_date'], unique=False)

    op.create_table('telegram_messages',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_name', sa.String(length=100), nullable=False),
        sa.Column('category', sa.String(length=50), nullable=True),
        sa.Column('message_id', sa.BigInteger(), nullable=False),
        sa.Column('message_text', sa.Text(), nullable=False),
        sa.Column('date', sa.DateTime(), nullable=True),
        sa.Column('media_url', sa.String(length=500), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('channel_name', 'message_id', name='uq_channel_message')
    )
    op.create_index(op.f('ix_telegram_messages_category'), 'telegram_messages', ['category'], unique=False)
    op.create_index(op.f('ix_telegram_messages_channel_name'), 'telegram_messages', ['channel_name'], unique=False)
    op.create_index(op.f('ix_telegram_messages_message_id'), 'telegram_messages', ['message_id'], unique=False)
    op.create_index(op.f('ix_telegram_messages_status'), 'telegram_messages', ['status'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_telegram_messages_status'), table_name='telegram_messages')
    op.drop_index(op.f('ix_telegram_messages_message_id'), table_name='telegram_messages')
    op.drop_index(op.f('ix_telegram_messages_channel_name'), table_name='telegram_messages')
    op.drop_index(op.f('ix_telegram_messages_category'), table_name='telegram_messages')
    op.drop_table('telegram_messages')

    op.drop_index(op.f('ix_scraping_logs_run_date'), table_name='scraping_logs')
    op.drop_index(op.f('ix_scraping_logs_channel_name'), table_name='scraping_logs')
    op.drop_index(op.f('ix_scraping_logs_category'), table_name='scraping_logs')
    op.drop_table('scraping_logs')

    op.drop_index(op.f('ix_scraping_error_logs_run_date'), table_name='scraping_error_logs')
    op.drop_index(op.f('ix_scraping_error_logs_channel_name'), table_name='scraping_error_logs')
    op.drop_index(op.f('ix_scraping_error_logs_category'), table_name='scraping_error_logs')
    op.drop_table('scraping_error_logs')

    op.drop_index(op.f('ix_archival_logs_table_name'), table_name='archival_logs')
    op.drop_index(op.f('ix_archival_logs_cutoff_date'), table_name='archival_logs')
    op.drop_index(op.f('ix_archival_logs_category'), table_name='archival_logs')
    op.drop_table('archival_logs')

    op.drop_index(op.f('ix_skipped_records_user_id'), table_name='skipped_records')
    op.drop_index(op.f('ix_skipped_records_clean_line_id'), table_name='skipped_records')
    op.drop_index(op.f('ix_skipped_records_annotation_type'), table_name='skipped_records')
    op.drop_table('skipped_records')

    op.drop_index(op.f('ix_annotation_results_user_id'), table_name='annotation_results')
    op.drop_index(op.f('ix_annotation_results_clean_line_id'), table_name='annotation_results')
    op.drop_index(op.f('ix_annotation_results_annotation_type'), table_name='annotation_results')
    op.drop_table('annotation_results')

    op.drop_table('users')

    op.drop_index(op.f('ix_cleaning_logs_run_date'), table_name='cleaning_logs')
    op.drop_index(op.f('ix_cleaning_logs_channel_name'), table_name='cleaning_logs')
    op.drop_index(op.f('ix_cleaning_logs_category'), table_name='cleaning_logs')
    op.drop_table('cleaning_logs')

    op.drop_index(op.f('ix_cleaning_error_logs_run_date'), table_name='cleaning_error_logs')
    op.drop_index(op.f('ix_cleaning_error_logs_channel_name'), table_name='cleaning_error_logs')
    op.drop_index(op.f('ix_cleaning_error_logs_category'), table_name='cleaning_error_logs')
    op.drop_table('cleaning_error_logs')

    op.drop_index(op.f('ix_clean_tele_text_telegram_message_id'), table_name='clean_tele_text')
    op.drop_index(op.f('ix_clean_tele_text_channel_name'), table_name='clean_tele_text')
    op.drop_index(op.f('ix_clean_tele_text_category'), table_name='clean_tele_text')
    op.drop_table('clean_tele_text')

    op.drop_index(op.f('ix_clean_tele_extra_info_message_id'), table_name='clean_tele_extra_info')
    op.drop_index(op.f('ix_clean_tele_extra_info_channel_name'), table_name='clean_tele_extra_info')
    op.drop_index(op.f('ix_clean_tele_extra_info_category'), table_name='clean_tele_extra_info')
    op.drop_table('clean_tele_extra_info')
