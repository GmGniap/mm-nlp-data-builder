#!/usr/bin/env python3
"""
One-Time Telegram Authorization Helper Script
==============================================
Run this script interactively in your terminal to authorize Telethon.
It will prompt for your phone number and login code, then save a `.session` file.

Usage:
  uv run python services/telegram_scraper/login_telegram.py
"""

import os
import sys
import asyncio
import yaml
from dotenv import load_dotenv

load_dotenv()

def load_config(config_file=None):
    if not config_file:
        config_file = os.getenv("CONFIG_PATH", "config.yaml")
    if os.path.isabs(config_file):
        target_path = config_file
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        target_path = os.path.join(base_dir, config_file)
    if not os.path.exists(target_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        target_path = os.path.join(base_dir, "config.example.yaml")
    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def authorize(output_string_session: bool = False, config_file: str = None):
    config = load_config(config_file)
    api_id = os.getenv("TELEGRAM_API_ID") or config.get("telegram", {}).get("api_id")
    api_hash = os.getenv("TELEGRAM_API_HASH") or config.get("telegram", {}).get("api_hash")
    session_name = config.get("telegram", {}).get("session_name", "telegram_scraper")

    session_path = os.path.join(os.path.dirname(__file__), session_name)

    print("\n" + "="*60)
    print(" 📲 Telegram Interactive Authorization Helper")
    print("="*60)
    print(f" API ID       : {api_id}")
    print(f" Session File : {session_path}.session")
    print(f" String Token : {output_string_session}")
    print("="*60)

    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
    except ImportError:
        print("❌ Telethon library is missing. Install with: uv pip install telethon")
        return

    if output_string_session:
        session_obj = StringSession()
    else:
        session_obj = session_path

    client = TelegramClient(session_obj, int(api_id), api_hash)
    await client.start()

    me = await client.get_me()
    print("\n✅ AUTHORIZATION SUCCESSFUL!")
    print(f" Signed in as : {me.first_name} (@{me.username or me.id})")
    
    if output_string_session:
        session_string = client.session.save()
        print("\n🔑 TELEGRAM_STRING_SESSION (Use in Airflow / Docker / Env Vars):")
        print("-" * 60)
        print(session_string)
        print("-" * 60)
        print("Copy the token above into your .env or Airflow Variables/Secrets as TELEGRAM_STRING_SESSION.\n")
    else:
        print(f" Session file created at: {session_path}.session")
        print(" You can now run the scraper automatically!\n")

    await client.disconnect()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Authorize Telegram Telethon session")
    parser.add_argument("--string-session", action="store_true", help="Generate and print a TELEGRAM_STRING_SESSION string for Docker/Airflow")
    parser.add_argument("--config", default=None, help="Path to config YAML")
    args = parser.parse_args()

    asyncio.run(authorize(output_string_session=args.string_session, config_file=args.config))
