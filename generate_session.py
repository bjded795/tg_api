import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = 22551545  # Your API ID
API_HASH = 'd8d697e4b63ee18a95c48eb875dd0947'  # Your API Hash
PHONE = '+918755365674'  # Your phone number

async def generate_session():
    async with TelegramClient(StringSession(), API_ID, API_HASH) as client:
        await client.start(phone=PHONE)
        session_string = client.session.save()
        print("Session string:", session_string)
        with open('session_string.txt', 'w') as f:
            f.write(session_string)
        print("Session string saved to session_string.txt")

if __name__ == '__main__':
    asyncio.run(generate_session())
