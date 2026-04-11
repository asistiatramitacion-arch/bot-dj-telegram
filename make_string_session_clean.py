import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 30868072
API_HASH = "5a701cba7a73380eee1b5bf75aca01c8"

async def main():
    print("Generando USERBOT_SESSION nueva...\n")
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        me = await client.get_me()
        print("✅ Sesión generada")
        print(f"Cuenta: {(me.first_name or '')} {(me.last_name or '')}".strip())
        print(f"ID: {me.id}")
        if me.username:
            print(f"@{me.username}")
        print("\nPEGA ESTA CADENA EN RAILWAY COMO USERBOT_SESSION:\n")
        print(client.session.save())

if __name__ == "__main__":
    asyncio.run(main())
