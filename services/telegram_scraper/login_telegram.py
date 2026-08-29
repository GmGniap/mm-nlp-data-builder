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

def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(base_dir, "config.yaml")
    if not os.path.exists(target_path):
        target_path = os.path.join(base_dir, "config.example.yaml")
    with open(target_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def authorize():
    config = load_config()
    api_id = os.getenv("TELEGRAM_API_ID") or config["telegram"].get("api_id")
    api_hash = os.getenv("TELEGRAM_API_HASH") or config["telegram"].get("api_hash")
    session_name = config["telegram"].get("session_name", "telegram_scraper")

    session_path = os.path.join(os.path.dirname(__file__), session_name)

    print("\n" + "="*60)
    print(" 📲 Telegram Interactive Authorization Helper")
    print("="*60)
    print(f" API ID       : {api_id}")
    print(f" Session File : {session_path}.session")
    print("="*60)

    try:
        from telethon import TelegramClient
    except ImportError:
        print("❌ Telethon library is missing. Install with: uv pip install telethon")
        return

    client = TelegramClient(session_path, int(api_id), api_hash)
    await client.start()

    me = await client.get_me()
    print("\n✅ AUTHORIZATION SUCCESSFUL!")
    print(f" Signed in as : {me.first_name} (@{me.username or me.id})")
    print(f" Session file created at: {session_path}.session")
    print(" You can now run the scraper automatically!\n")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(authorize())
