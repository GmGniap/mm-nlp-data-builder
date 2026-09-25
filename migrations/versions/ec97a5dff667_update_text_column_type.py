"""Update TEXT column type

Revision ID: ec97a5dff667
Revises: 0001_initial_schema
Create Date: 2026-09-26 04:05:57.012112

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ec97a5dff667'
down_revision: Union[str, Sequence[str], None] = '0001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema safely without dropping existing tables."""
    try:
        op.create_index(op.f('ix_archival_logs_category'), 'archival_logs', ['category'], unique=False)
    except Exception:
        pass

    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite" if bind else False
    context = op.get_context()
    is_offline = getattr(context, "as_sql", False)

    if is_sqlite:
        if not is_offline:
            with op.batch_alter_table('clean_tele_extra_info') as batch_op:
                batch_op.alter_column(
                    'original_short_note',
                    existing_type=sa.VARCHAR(length=255),
                    type_=sa.Text(),
                    existing_nullable=True,
                )
            with op.batch_alter_table('scraping_error_logs') as batch_op:
                batch_op.alter_column(
                    'category',
                    existing_type=sa.VARCHAR(length=50),
                    nullable=True,
                )
    else:
        # PostgreSQL native safe alter
        op.alter_column(
            'clean_tele_extra_info',
            'original_short_note',
            existing_type=sa.VARCHAR(length=255),
            type_=sa.Text(),
            existing_nullable=True,
        )
        op.alter_column(
            'scraping_error_logs',
            'category',
            existing_type=sa.VARCHAR(length=50),
            nullable=True,
        )


def downgrade() -> None:
    """Downgrade schema safely."""
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite" if bind else False
    context = op.get_context()
    is_offline = getattr(context, "as_sql", False)

    if is_sqlite:
        if not is_offline:
            with op.batch_alter_table('scraping_error_logs') as batch_op:
                batch_op.alter_column(
                    'category',
                    existing_type=sa.VARCHAR(length=50),
                    nullable=False,
                )
            with op.batch_alter_table('clean_tele_extra_info') as batch_op:
                batch_op.alter_column(
                    'original_short_note',
                    existing_type=sa.Text(),
                    type_=sa.VARCHAR(length=255),
                    existing_nullable=True,
                )
    else:
        op.alter_column(
            'scraping_error_logs',
            'category',
            existing_type=sa.VARCHAR(length=50),
            nullable=False,
        )
        op.alter_column(
            'clean_tele_extra_info',
            'original_short_note',
            existing_type=sa.Text(),
            type_=sa.VARCHAR(length=255),
            existing_nullable=True,
        )

    try:
        op.drop_index(op.f('ix_archival_logs_category'), table_name='archival_logs')
    except Exception:
        pass
