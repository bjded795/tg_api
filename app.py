import logging
import asyncio
import re
import base64
import signal
import sys
import os
import time
import traceback
import threading
from flask import Flask, jsonify, Response, request, stream_with_context
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, InputMessagesFilterVideo, DocumentAttributeFilename
from telethon.errors import SessionPasswordNeededError, FileReferenceExpiredError, FloodWaitError, RPCError
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

# Telegram API credentials (hardcoded)
API_ID = 22551545
API_HASH = 'd8d697e4b63ee18a95c48eb875dd0947'
PHONE = '+918755365674'
SESSION_FILE = 'telegram_session.session'

# Global variables
client = None
loop = None
is_authenticated = False

def load_session_string():
    """Load session string from file."""
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, 'r') as f:
                session_string = f.read().strip()
                logger.info("Loaded session string from file")
                return session_string
        except Exception as e:
            logger.error(f"Error loading session: {str(e)}")
    logger.warning(f"Session file {SESSION_FILE} not found")
    return ''

def setup_async():
    """Setup async event loop and signal handlers."""
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown_handler(signum, frame):
        logger.info("Shutting down gracefully...")
        try:
            if client and client.is_connected():
                run_async_in_thread(client.disconnect(), timeout=10)
        except:
            pass
        loop.call_soon_threadsafe(loop.stop)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

def start_loop():
    """Run the event loop forever in a separate thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()

def sync_async(f):
    """Decorator to run async functions synchronously."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        return run_async_in_thread(f(*args, **kwargs))
    return wrapper

def run_async_in_thread(coro, timeout=30):
    """Run an async coroutine in a thread-safe manner using the global loop."""
    try:
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"Async operation timed out after {timeout} seconds")
        raise
    except Exception as e:
        logger.error(f"Async operation failed: {str(e)}\n{traceback.format_exc()}")
        raise

async def init_client():
    """Initialize and authenticate Telegram client."""
    global client, is_authenticated

    if client and client.is_connected():
        return

    session_string = load_session_string()
    if not session_string:
        logger.error("No valid session string found. Authentication required.")
        raise Exception("Session file missing or invalid")

    client = TelegramClient(
        StringSession(session_string),
        API_ID,
        API_HASH,
        loop=loop,
        connection_retries=5,
        timeout=30
    )

    try:
        logger.info("Connecting to Telegram...")
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("Client not authorized. Session string invalid or expired.")
            raise Exception("Invalid or unauthorized session string")
        is_authenticated = True
        logger.info("Telegram client authenticated successfully")
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}\n{traceback.format_exc()}")
        is_authenticated = False
        if client.is_connected():
            await client.disconnect()
        raise

async def fetch_groups():
    """Fetch all groups/dialogs."""
    if not client or not client.is_connected():
        await init_client()

    groups = []
    try:
        logger.info("Fetching groups...")
        async for dialog in client.iter_dialogs():
            if dialog.is_group or dialog.is_channel:
                entity = dialog.entity
                if hasattr(entity, 'id') and hasattr(entity, 'title'):
                    group_id = entity.id
                    if not str(group_id).startswith('-100'):
                        group_id = int(f"-100{abs(group_id)}")
                    groups.append({
                        'id': str(group_id),
                        'name': entity.title,
                        'access_hash': entity.access_hash if hasattr(entity, 'access_hash') else 0
                    })
        logger.info(f"Fetched {len(groups)} groups/channels")
        return groups
    except RPCError as e:
        logger.error(f"Telegram RPC error fetching groups: {str(e)}\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Error fetching groups: {str(e)}\n{traceback.format_exc()}")
        raise

async def fetch_videos(group_id, limit=20):
    """Fetch videos from a specific group."""
    if not client or not client.is_connected():
        await init_client()

    video_messages = []
    try:
        logger.info(f"Fetching videos for group {group_id}...")
        if not str(group_id).startswith('-100'):
            group_id = int(f"-100{abs(int(group_id))}")
        else:
            group_id = int(group_id)

        async for message in client.iter_messages(
            group_id,
            filter=InputMessagesFilterVideo,
            limit=limit
        ):
            if message.media and isinstance(message.media, MessageMediaDocument):
                document = message.media.document
                if hasattr(document, 'mime_type') and 'video' in document.mime_type:
                    file_name = f"video_{message.id}.mp4"
                    for attr in document.attributes:
                        if isinstance(attr, DocumentAttributeFilename):
                            file_name = attr.file_name
                            break
                    video_messages.append({
                        'group_id': str(group_id),
                        'message_id': message.id,
                        'file_name': file_name,
                        'file_size': document.size,
                        'mime_type': document.mime_type,
                        'access_hash': document.access_hash,
                        'file_reference': base64.b64encode(document.file_reference).decode('utf-8') if document.file_reference else None
                    })
        logger.info(f"Fetched {len(video_messages)} videos from group {group_id}")
        return video_messages
    except RPCError as e:
        logger.error(f"Telegram RPC error fetching videos: {str(e)}\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Error fetching videos: {str(e)}\n{traceback.format_exc()}")
        raise

async def get_fresh_message(group_id, message_id):
    """Get fresh message reference."""
    for attempt in range(3):
        try:
            logger.info(f"Fetching message {message_id} from group {group_id}, attempt {attempt + 1}")
            if not client or not client.is_connected():
                await init_client()
            message = await client.get_messages(group_id, ids=message_id)
            if message:
                logger.info(f"Successfully fetched message {message_id}")
                return message
            logger.warning(f"No message found for ID {message_id} in group {group_id}")
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

async def download_chunk(group_id, message_id, offset, limit):
    """Download specific chunk of video."""
    for attempt in range(3):
        try:
            logger.info(f"Downloading chunk for message {message_id} at offset {offset}, attempt {attempt + 1}")
            message = await get_fresh_message(group_id, message_id)
            if not message or not message.media:
                logger.error(f"No media found for message {message_id} in group {group_id}")
                return None

            # Initialize variables for chunk download
            bytes_remaining = limit
            collected_data = bytearray()

            # Use iter_download for partial downloads
            async for chunk in client.iter_download(
                file=message.media,
                offset=offset,
                request_size=1024 * 1024  # 1MB chunks
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
            logger.info(f"Successfully downloaded chunk at offset {offset}, size {len(collected_data)} bytes")
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

@app.route('/groups')
@sync_async
async def list_groups():
    """List all available groups."""
    try:
        groups = await fetch_groups()
        return jsonify({
            'status': 'success',
            'groups': groups,
            'count': len(groups)
        })
    except Exception as e:
        logger.error(f"Error in list_groups: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/videos/<group_id>')
@sync_async
async def list_videos(group_id):
    """List videos in a group."""
    try:
        videos = await fetch_videos(group_id)
        if not videos:
            return jsonify({
                'status': 'error',
                'message': 'No videos found'
            }), 404
        for idx, video in enumerate(videos):
            video['play_url'] = f'/{video["group_id"]}/{idx}'
        return jsonify({
            'status': 'success',
            'videos': videos,
            'count': len(videos)
        })
    except Exception as e:
        logger.error(f"Error in list_videos: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/<group_id>/<int:video_idx>')
def stream_video(group_id, video_idx):
    """Stream video endpoint."""
    try:
        logger.info(f"Starting stream for group_id: {group_id}, video_idx: {video_idx}")
        # Fetch videos
        start_time = time.time()
        videos = run_async_in_thread(fetch_videos(group_id))
        logger.info(f"Fetched videos in {time.time() - start_time:.2f} seconds")
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
                headers['Content-Length': str(end - start + 1)]
            else:
                logger.error(f"Invalid range header: {range_header}")
                return Response(status=416, headers={'Content-Range': f'bytes */{file_size}'})

        def generate():
            nonlocal start, end
            chunk_size = 1024 * 1024  # 1MB chunks
            current_offset = start
            while current_offset <= end:
                read_size = min(chunk_size, end - current_offset + 1)
                logger.info(f"Downloading chunk at offset {current_offset}, size {read_size}")
                chunk_start = time.time()
                chunk = run_async_in_thread(
                    download_chunk(
                        int(video['group_id']),
                        video['message_id'],
                        current_offset,
                        read_size
                    )
                )
                logger.info(f"Downloaded chunk in {time.time() - chunk_start:.2f} seconds")
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

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'authenticated': is_authenticated
    })

def main():
    """Main application entry point."""
    try:
        setup_async()
        # Start the event loop in a separate thread
        loop_thread = threading.Thread(target=start_loop)
        loop_thread.daemon = True  # Ensure thread exits when main process does
        loop_thread.start()
        
        # Initialize Telegram client using the thread-safe method
        run_async_in_thread(init_client(), timeout=30)
        
        # Note: Flask app is run by Gunicorn in production, not here
    except Exception as e:
        logger.error(f"Application failed: {str(e)}\n{traceback.format_exc()}")
        try:
            if client and client.is_connected():
                run_async_in_thread(client.disconnect(), timeout=10)
        except:
            pass
        sys.exit(1)

if __name__ == '__main__':
    # For local development only
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True)
