import asyncio
import logging
from flask import Flask, Response, jsonify
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeVideo
import os

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)

# Telegram client configuration
API_ID = 22551545
API_HASH = 'd8d697e4b63ee18a95c48eb875dd0947'
PHONE = '+918755365674'
SESSION_FILE = 'telegram_session.session'

# Initialize asyncio loop globally
loop = asyncio.get_event_loop_policy().new_event_loop()
client = None

async def init_telegram_client():
    global client
    if os.path.exists(SESSION_FILE):
        logger.info("Loaded session string from file")
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH, loop=loop)
        await client.start(phone=PHONE)
        if await client.is_user_authorized():
            logger.info("Telegram client authenticated successfully")
        else:
            logger.error("Telegram client not authenticated")
    else:
        logger.error("Session file missing or invalid")
        raise Exception("Session file missing or invalid")

def run_async_in_thread(coro):
    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=30)
    except Exception as e:
        logger.error(f"Async operation failed: {str(e)}")
        raise

# Initialize client on app startup
try:
    run_async_in_thread(init_telegram_client())
except Exception as e:
    logger.error(f"Failed to initialize Telegram client: {str(e)}")

@app.route('/')
def index():
    return jsonify({
        "status": "success",
        "message": "Welcome to the Telegram Video Streaming API",
        "endpoints": {
            "/groups": "List all Telegram groups/channels",
            "/videos/<group_id>": "List videos in a specific group",
            "/<group_id>/<video_idx>": "Stream a specific video",
            "/health": "Check server status and authentication"
        }
    })

@app.route('/health')
def health():
    authenticated = client is not None and client.is_connected()
    return jsonify({
        "status": "ok",
        "authenticated": authenticated
    })

@app.route('/groups')
def get_groups():
    async def fetch_groups():
        groups = []
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                groups.append({
                    "id": str(dialog.id),
                    "name": dialog.title,
                    "access_hash": dialog.entity.access_hash
                })
        return groups

    try:
        groups = run_async_in_thread(fetch_groups())
        return jsonify({"status": "success", "groups": groups})
    except Exception as e:
        logger.error(f"Error fetching groups: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/videos/<group_id>')
def get_videos(group_id):
    async def fetch_videos():
        videos = []
        entity = await client.get_entity(int(group_id))
        async for message in client.iter_messages(entity, filter=lambda m: m.video):
            for attr in message.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                if isinstance(attr, DocumentAttributeVideo):
                    duration = attr.duration
            videos.append({
                "group_id": group_id,
                "message_id": message.id,
                "file_name": file_name,
                "file_size": message.document.size,
                "mime_type": message.document.mime_type,
                "access_hash": message.document.access_hash,
                "file_reference": message.document.file_reference.hex(),
                "play_url": f"/{group_id}/{len(videos)}"
            })
        return videos

    try:
        videos = run_async_in_thread(fetch_videos())
        return jsonify({"status": "success", "videos": videos})
    except Exception as e:
        logger.error(f"Error fetching videos: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/<group_id>/<int:video_idx>')
def stream_video(group_id, video_idx):
    async def fetch_video():
        entity = await client.get_entity(int(group_id))
        async for message in client.iter_messages(entity, filter=lambda m: m.video, limit=video_idx + 1):
            messages = [m async for m in client.iter_messages(entity, filter=lambda m: m.video, limit=video_idx + 1)]
            if video_idx < len(messages):
                video_message = messages[video_idx]
                return video_message

    try:
        video_message = run_async_in_thread(fetch_video())
        if not video_message:
            return jsonify({"status": "error", "message": "Video not found"}), 404

        def generate():
            async def stream():
                async with client:
                    async for chunk in client.iter_download(video_message.document, chunk_size=1024*1024):
                        yield chunk
            return run_async_in_thread(stream())

        return Response(generate(), mimetype=video_message.document.mime_type)
    except Exception as e:
        logger.error(f"Error streaming video: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting Flask server on port 8000")
    app.run(host='0.0.0.0', port=8000)
