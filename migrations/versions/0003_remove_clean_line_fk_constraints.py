"""Remove clean_line_id foreign key constraints

Revision ID: 0003_remove_clean_line_fk
Revises: ec97a5dff667
Create Date: 2026-09-26 04:18:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = '0003_remove_clean_line_fk'
down_revision: Union[str, Sequence[str], None] = 'ec97a5dff667'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Decouple clean_tele_text and annotation tables by dropping DB-level FK constraints.
    clean_line_id remains an indexed logical reference without locking or dependency blocking.
    """
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite" if bind else False
    context = op.get_context()
    is_offline = getattr(context, "as_sql", False)

    if is_sqlite:
        if not is_offline and bind:
            insp = inspect(bind)
            for tbl, fk_name in [
                ('annotation_results', 'fk_annotation_results_clean_line_id'),
                ('skipped_records', 'fk_skipped_records_clean_line_id'),
            ]:
                try:
                    fks = insp.get_foreign_keys(tbl)
                    has_fk = any('clean_line_id' in fk.get('constrained_columns', []) for fk in fks)
                    if has_fk:
                        with op.batch_alter_table(tbl) as batch_op:
                            batch_op.drop_constraint(fk_name, type_='foreignkey')
                except Exception:
                    pass
    else:
        # PostgreSQL: Drop constraints safely and idempotently
        for fk_name, tbl_name in [
            ('annotation_results_clean_line_id_fkey', 'annotation_results'),
            ('fk_annotation_results_clean_line_id', 'annotation_results'),
            ('skipped_records_clean_line_id_fkey', 'skipped_records'),
            ('fk_skipped_records_clean_line_id', 'skipped_records'),
        ]:
            op.execute(sa.text(f'ALTER TABLE {tbl_name} DROP CONSTRAINT IF EXISTS {fk_name}'))


def downgrade() -> None:
    """
    Recreate DB-level FK constraints if this migration is rolled back.
    """
    bind = op.get_bind()
    is_sqlite = bind.dialect.name == "sqlite" if bind else False
    context = op.get_context()
    is_offline = getattr(context, "as_sql", False)

    if is_sqlite:
        if not is_offline:
            try:
                with op.batch_alter_table('annotation_results') as batch_op:
                    batch_op.create_foreign_key(
                        'fk_annotation_results_clean_line_id',
                        'clean_tele_text',
                        ['clean_line_id'],
                        ['id'],
                    )
            except Exception:
                pass
            try:
                with op.batch_alter_table('skipped_records') as batch_op:
                    batch_op.create_foreign_key(
                        'fk_skipped_records_clean_line_id',
                        'clean_tele_text',
                        ['clean_line_id'],
                        ['id'],
                    )
            except Exception:
                pass
    else:
        # PostgreSQL
        for fk_name, tbl_name in [
            ('annotation_results_clean_line_id_fkey', 'annotation_results'),
            ('skipped_records_clean_line_id_fkey', 'skipped_records'),
        ]:
            try:
                op.create_foreign_key(
                    fk_name,
                    tbl_name,
                    'clean_tele_text',
                    ['clean_line_id'],
                    ['id'],
                )
            except Exception:
                pass
