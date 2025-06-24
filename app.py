import logging
import asyncio
import re
import base64
import sys
import os
import time
import traceback
import threading
from flask import Flask, jsonify, Response, request, stream_with_context
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, InputMessagesFilterVideo, DocumentAttributeFilename
from telethon.errors import FileReferenceExpiredError, FloodWaitError, RPCError
from telethon.sessions import StringSession
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
logger.info("Flask app initialized")

# Telegram API credentials
API_ID = 22551545
API_HASH = 'd8d697e4b63ee18a95c48eb875dd0947'
PHONE = '+918755365674'

# Hardcoded session string
SESSION_STRING = '1BVtsOKsBuxZ4b-siBx55oOKENioFw5vijWcIOLPsrcsvRndcOKW8L1DNFoK7f_kx65baciqaFcKQVp8VbzT_DMibuKAm2dCPe5j-eTEsbi5DF88CweAfdLmajrwzI06Ew2dzceJEJXwNx7OwJL0vrF6Z14cWEBtfetbUWHUDYO6Eu85IQdJNbaSzB2WRDhhXwOBTZAAN8WbGxhHrfNytmCe7_G7R1rYmVsx4wAWb9-H6kJejZiTvkMIJUxJo7djMIu4wxG3yKCJZy-SMcBITPYX2wWHkO0BZOW8cXAsKqjKM4LcoDFcrB0CeooiS82rYV5ja-cCXfvuxgHJV1z8J5rSkXfGAQvM='

# Global variables
client = None
loop = None
is_authenticated = False
loop_thread = None
initialized = False

def setup_async():
    """Setup async event loop in a separate thread."""
    global loop, loop_thread
    if loop is not None and loop.is_running():
        logger.info("Event loop already running")
        return
        
    loop = asyncio.new_event_loop()
    def start_loop():
        asyncio.set_event_loop(loop)
        logger.info("Starting event loop thread")
        loop.run_forever()
    loop_thread = threading.Thread(target=start_loop, daemon=True)
    loop_thread.start()
    logger.info("Event loop thread started")

def run_async_in_thread(coro, timeout=60):
    """Run an async coroutine in a thread-safe manner."""
    if loop is None:
        logger.error("Event loop not initialized")
        setup_async()
        if loop is None:
            raise RuntimeError("Event loop not initialized")
    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"Async operation timed out after {timeout} seconds")
        raise
    except Exception as e:
        logger.error(f"Async operation failed: {str(e)}\n{traceback.format_exc()}")
        raise

def sync_async(f):
    """Decorator to run async functions synchronously."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        return run_async_in_thread(f(*args, **kwargs))
    return wrapper

async def init_client():
    """Initialize and authenticate Telegram client."""
    global client, is_authenticated
    
    try:
        logger.info("Initializing Telegram client with hardcoded session...")
        session = StringSession(SESSION_STRING)
        client = TelegramClient(session, API_ID, API_HASH, loop=loop)
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error("Client not authorized")
            raise Exception("Telegram client not authenticated")
            
        is_authenticated = True
        logger.info("Telegram client authenticated successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Telegram client: {str(e)}\n{traceback.format_exc()}")
        is_authenticated = False
        if client and client.is_connected():
            await client.disconnect()
        raise

def ensure_initialized(f):
    """Decorator to ensure app is initialized before handling requests."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        global initialized
        if not initialized:
            try:
                logger.info("Initializing application...")
                setup_async()
                run_async_in_thread(init_client())
                initialized = True
            except Exception as e:
                logger.error(f"Initialization failed: {str(e)}\n{traceback.format_exc()}")
                return jsonify({
                    'status': 'error',
                    'message': 'Initialization failed'
                }), 500
        return f(*args, **kwargs)
    return wrapper

@app.route('/')
def index():
    """Root endpoint to provide API guidance."""
    return jsonify({
        'status': 'success',
        'message': 'Welcome to the Telegram Video Streaming API',
        'endpoints': {
            '/groups': 'List all Telegram groups/channels',
            '/videos/<group_id>': 'List videos in a specific group',
            '/<group_id>/<video_idx>': 'Stream a specific video',
            '/health': 'Check server status and authentication'
        }
    })

@app.route('/health')
@ensure_initialized
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'authenticated': is_authenticated
    })

@app.route('/groups')
@ensure_initialized
@sync_async
async def list_groups():
    """List all available groups."""
    if not client or not client.is_connected():
        await init_client()
    groups = []
    try:
        logger.info("Fetching groups...")
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                entity = dialog.entity
                group_id = entity.id
                if not str(group_id).startswith('-100'):
                    group_id = int(f"-100{abs(group_id)}")
                groups.append({
                    'id': str(group_id),
                    'name': entity.title,
                    'access_hash': entity.access_hash if hasattr(entity, 'access_hash') else 0
                })
        logger.info(f"Fetched {len(groups)} groups/channels")
        return {
            'status': 'success',
            'groups': groups,
            'count': len(groups)
        }
    except RPCError as e:
        logger.error(f"Telegram RPC error fetching groups: {str(e)}\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Error fetching groups: {str(e)}\n{traceback.format_exc()}")
        raise

async def fetch_videos(group_id):
    """Helper function to fetch videos from a group."""
    if not client or not client.is_connected():
        await init_client()
    videos = []
    try:
        logger.info(f"Fetching videos for group {group_id}...")
        if not str(group_id).startswith('-100'):
            group_id_int = int(f"-100{abs(int(group_id))}")
        else:
            group_id_int = int(group_id)
        async for message in client.iter_messages(
            group_id_int,
            filter=InputMessagesFilterVideo,
            limit=20
        ):
            if message.media and isinstance(message.media, MessageMediaDocument):
                document = message.media.document
                if hasattr(document, 'mime_type') and 'video' in document.mime_type:
                    file_name = f"video_{message.id}.mp4"
                    for attr in document.attributes:
                        if isinstance(attr, DocumentAttributeFilename):
                            file_name = attr.file_name
                            break
                    videos.append({
                        'group_id': str(group_id_int),
                        'message_id': message.id,
                        'file_name': file_name,
                        'file_size': document.size,
                        'mime_type': document.mime_type,
                        'access_hash': document.access_hash,
                        'file_reference': base64.b64encode(document.file_reference).decode('utf-8') if document.file_reference else None,
                        'play_url': f'/{str(group_id_int)}/{len(videos)}'
                    })
        logger.info(f"Fetched {len(videos)} videos from group {group_id}")
        return videos
    except RPCError as e:
        logger.error(f"Telegram RPC error fetching videos: {str(e)}\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Error fetching videos: {str(e)}\n{traceback.format_exc()}")
        raise

@app.route('/videos/<group_id>')
@ensure_initialized
@sync_async
async def list_videos(group_id):
    """List videos in a group."""
    videos = await fetch_videos(group_id)
    if not videos:
        return {
            'status': 'error',
            'message': 'No videos found'
        }
    return {
        'status': 'success',
        'videos': videos,
        'count': len(videos)
    }

@app.route('/<group_id>/<int:video_idx>')
@ensure_initialized
def stream_video(group_id, video_idx):
    """Stream video endpoint."""
    async def get_fresh_message(group_id_int, message_id):
        for attempt in range(3):
            try:
                logger.info(f"Fetching message {message_id} from group {group_id_int}, attempt {attempt + 1}")
                if not client or not client.is_connected():
                    await init_client()
                message = await client.get_messages(group_id_int, ids=message_id)
                if message:
                    logger.info(f"Successfully fetched message {message_id}")
                    return message
                logger.warning(f"No message found for ID {message_id} in group {group_id_int}")
            except FileReferenceExpiredError:
                logger.warning(f"File reference expired, retrying... (attempt {attempt + 1})")
                await asyncio.sleep(0.5)
            except RPCError as e:
                logger.error(f"Telegram RPC error getting message {message_id}: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error getting fresh message for ID {message_id}: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(0.5)
        logger.error(f"Failed to get fresh message for ID {message_id} after 3 attempts")
        return None

    async def download_chunk(group_id_int, message_id, offset, limit):
        for attempt in range(3):
            try:
                logger.info(f"Downloading chunk for message {message_id} at offset {offset}, attempt {attempt + 1}")
                message = await get_fresh_message(group_id_int, message_id)
                if not message or not message.media:
                    logger.error(f"No media found for message {message_id} in group {group_id_int}")
                    return None
                bytes_remaining = limit
                collected_data = bytearray()
                async for chunk in client.iter_download(
                    file=message.media,
                    offset=offset,
                    request_size=1024 * 1024
                ):
                    chunk_size = len(chunk)
                    if chunk_size > bytes_remaining:
                        collected_data.extend(chunk[:bytes_remaining])
                        bytes_remaining = 0
                        break
                    collected_data.extend(chunk)
                    bytes_remaining -= chunk_size
                    if bytes_remaining <= 0:
                        break
                if not collected_data:
                    logger.error(f"Failed to download chunk at offset {offset}")
                    return None
                logger.info(f"Downloaded chunk at offset {offset}, size {len(collected_data)} bytes")
                return bytes(collected_data)
            except FileReferenceExpiredError:
                logger.warning(f"File reference expired, retrying... (attempt {attempt + 1})")
                await asyncio.sleep(0.5)
            except FloodWaitError as e:
                wait_time = min(e.seconds, 5)
                logger.warning(f"Flood wait, sleeping for {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except RPCError as e:
                logger.error(f"Telegram RPC error downloading chunk: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error downloading chunk: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(0.5)
        logger.error(f"Failed to download chunk after 3 attempts")
        return None

    try:
        logger.info(f"Starting stream for group_id: {group_id}, video_idx: {video_idx}")
        videos = run_async_in_thread(fetch_videos(group_id))
        if video_idx >= len(videos):
            logger.error(f"Video index {video_idx} out of range for group {group_id}")
            return jsonify({
                'status': 'error',
                'message': 'Video index out of range'
            }), 404
        video = videos[video_idx]
        if video['group_id'] != group_id:
            logger.error(f"Video group_id {video['group_id']} does not match requested {group_id}")
            return jsonify({
                'status': 'error',
                'message': 'Video not found in specified group'
            }), 404
        file_size = video['file_size']
        range_header = request.headers.get('Range', None)
        start = 0
        end = file_size - 1
        status_code = 200
        headers = {
            'Content-Type': video['mime_type'],
            'Content-Disposition': f'inline; filename="{video["file_name"]}"',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(file_size)
        }
        if range_header:
            match = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if match:
                start = int(match.group(1))
                if match.group(2):
                    end = min(int(match.group(2)), file_size - 1)
                status_code = 206
                headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
                headers['Content-Length'] = str(end - start + 1)
            else:
                logger.error(f"Invalid range header: {range_header}")
                return Response(status=416, headers={'Content-Range': f'bytes */{file_size}'})

        def generate():
            nonlocal start, end
            chunk_size = 1024 * 1024
            current_offset = start
            while current_offset <= end:
                read_size = min(chunk_size, end - current_offset + 1)
                logger.info(f"Downloading chunk at offset {current_offset}, size {read_size}")
                chunk = run_async_in_thread(
                    download_chunk(
                        int(video['group_id']),
                        video['message_id'],
                        current_offset,
                        read_size
                    )
                )
                if not chunk:
                    logger.error(f"Empty chunk at offset {current_offset}")
                    break
                yield chunk
                current_offset += len(chunk)

        logger.info(f"Streaming video: {video['file_name']}, size: {file_size} bytes")
        return Response(
            stream_with_context(generate()),
            status=status_code,
            headers=headers,
            content_type=video['mime_type'],
            direct_passthrough=True
        )
    except Exception as e:
        logger.error(f"Error in stream_video: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': str(e) or "Unknown error occurred"
        }), 500

def main():
    """Main application entry point."""
    try:
        # Initialize immediately for CLI runs
        if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or __name__ == '__main__':
            setup_async()
            run_async_in_thread(init_client())
            initialized = True
            
        logger.info("Starting Flask server")
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000)), threaded=True)
    except Exception as e:
        logger.error(f"Application failed: {str(e)}\n{traceback.format_exc()}")
        if client and client.is_connected():
            run_async_in_thread(client.disconnect(), timeout=10)
        sys.exit(1)

if __name__ == '__main__':
    main()
