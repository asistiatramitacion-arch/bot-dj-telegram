import asyncio
import os
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "djplan_userbot")

async def main():
    if not API_ID or not API_HASH:
        raise RuntimeError("Pon API_ID y API_HASH como variables de entorno antes de ejecutar este script.")

    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        print("\n=== USERBOT SESSION ===\n")
        print(StringSession.save(client.session))
        print("\nGuárdala en Railway como USERBOT_SESSION\n")

if __name__ == "__main__":
    asyncio.run(main())
