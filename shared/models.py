"""
shared/models.py  —  DEPRECATED / DO NOT USE
=============================================
This file has been split into service-specific model modules to reflect
the two-database architecture (both on the same Neon PostgreSQL server
but owned by separate services):

  shared/scraper_models.py
      TelegramMessage, ScrapingLog, init_scraper_db()
      → Used by: services/telegram_scraper/storage.py
                 services/telegram_scraper/cleaner.py

  shared/annotation_models.py
      User, CleanTeleText, AnnotationTag, init_annotation_db()
      → Used by: services/telegram_scraper/cleaner.py  (write side)
                 services/nlp_annotation_app/models.py

Do NOT import from this file in new code.
"""

raise ImportError(
    "shared.models is deprecated. "
    "Import from shared.scraper_models or shared.annotation_models instead."
)
