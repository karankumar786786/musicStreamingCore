"""
Audio Processing Pipeline for Music Streaming
Processes audio files from S3, generates HLS streams and captions
"""

import os
import json
import time
import shutil
import subprocess
import urllib.parse
import logging
import mimetypes
from pathlib import Path
from typing import Dict, List, Optional
import re

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from faster_whisper import WhisperModel
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ---------------- LOGGING SETUP ----------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------- CONFIG (Environment Variables) ----------------

# AWS Configuration - NO HARDCODED CREDENTIALS
SQS_URL = os.getenv("SQS_URL")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# S3 Buckets
TEMP_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")
PRODUCTION_BUCKET_NAME = os.getenv("PRODUCTION_BUCKET_NAME")

# Validate required environment variables
REQUIRED_ENV_VARS = {
    "SQS_URL": SQS_URL,
    "TEMP_BUCKET_NAME": TEMP_BUCKET_NAME,
    "PRODUCTION_BUCKET_NAME": PRODUCTION_BUCKET_NAME,
}

for var_name, var_value in REQUIRED_ENV_VARS.items():
    if not var_value:
        raise EnvironmentError(f"❌ Required environment variable {var_name} is not set")

# Processing Configuration
BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "rustfs-downloads"
WORK_DIR = BASE_DIR / "work"

DOWNLOAD_DIR.mkdir(exist_ok=True)
WORK_DIR.mkdir(exist_ok=True)

# Audio Configuration
AUDIO_EXTENSIONS = {".mp3", ".wav", ".aac", ".m4a", ".flac", ".ogg", ".opus"}
MAX_FILE_SIZE_MB = 500
MAX_DURATION_SECONDS = 3600

# HLS Configuration
HLS_SEGMENT_DURATION = 6
HLS_QUALITIES = {
    "64k": "64000",
    "128k": "128000",
    "256k": "256000"
}

# Whisper Configuration
WHISPER_MODEL_SIZE = "medium"
WHISPER_DEVICE = "cpu"
WHISPER_COMPUTE_TYPE = "int8"

# Language names for subtitle metadata
LANGUAGE_NAMES = {
    "en": "English",
    "hi": "Hindi",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
    "ru": "Russian",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "ar": "Arabic",
    "bn": "Bengali",
    "ta": "Tamil",
    "te": "Telugu",
    "mr": "Marathi",
}

# ---------------- GLOBAL MODEL INIT ----------------

logger.info("🤖 Initializing Whisper model...")
try:
    WHISPER_MODEL = WhisperModel(WHISPER_MODEL_SIZE, device=WHISPER_DEVICE, compute_type=WHISPER_COMPUTE_TYPE)
    logger.info("✅ Whisper model initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize Whisper model: {e}")
    raise

# ---------------- AWS CLIENTS ----------------

boto_config = Config(
    region_name=AWS_REGION,
    retries={'max_attempts': 3, 'mode': 'adaptive'}
)

try:
    s3 = boto3.client('s3', config=boto_config)
    sqs = boto3.client('sqs', config=boto_config)
    logger.info("✅ AWS clients initialized successfully")
except Exception as e:
    logger.error(f"❌ Failed to initialize AWS clients: {e}")
    raise

# ---------------- UTILS ----------------

def slugify(value: str) -> str:
    """Convert string to a safe filename slug."""
    if not value:
        return ""
    value = re.sub(r'[^\x00-\x7F]+', '', value)
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    value = re.sub(r'[-\s]+', '-', value)
    return value


def is_audio_file(filename: str) -> bool:
    """Check if file has a valid audio extension."""
    return any(filename.lower().endswith(ext) for ext in AUDIO_EXTENSIONS)


def check_disk_space(required_mb: float) -> bool:
    """Check if sufficient disk space is available."""
    stat = shutil.disk_usage(WORK_DIR)
    available_mb = stat.free / (1024 * 1024)
    
    if available_mb < required_mb * 2:
        logger.warning(f"⚠️ Low disk space: {available_mb:.2f}MB available, {required_mb*2:.2f}MB required")
        return False
    return True


def get_audio_duration(audio_path: Path) -> float:
    """Get audio duration in seconds using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(audio_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        return duration
    except (subprocess.CalledProcessError, ValueError) as e:
        logger.warning(f"⚠️ Could not get audio duration: {e}")
        return 0.0


def run_ffmpeg(cmd: list, description: str = "FFmpeg operation") -> None:
    try:
        logger.debug(f"Running {description}...")
        # Removing capture_output allows FFmpeg to stream its own progress to the console
        subprocess.run(cmd, check=True) 
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ FFmpeg failed for {description}")
        raise RuntimeError(f"{description} failed")

def format_ts(sec: float) -> str:
    """Format seconds to VTT timestamp format (HH:MM:SS.mmm)."""
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return f"{h:02}:{m:02}:{s:06.3f}"


def words_to_karaoke_vtt(words: List, start: float, end: float) -> str:
    """Convert word-level timestamps to karaoke-style VTT format."""
    line = ""
    for w in words:
        # faster-whisper returns Word namedtuples, not dicts
        w_start = getattr(w, "start", start)
        w_word = getattr(w, "word", "").strip()
        ts = format_ts(w_start)
        line += f"<{ts}>{w_word} "
    return f"{format_ts(start)} --> {format_ts(end)}\n{line.strip()}\n\n"

def generate_transcription(audio_path: Path, vtt_out: Path, language: Optional[str] = None) -> str:
    """Generate karaoke-style VTT subtitles with auto-language detection."""
    logger.info(f"🎙️ Starting transcription (Auto-detecting language)...")
    
    try:
        # Setting language=None enables auto-detection
        segments, info = WHISPER_MODEL.transcribe(
            str(audio_path), 
            word_timestamps=True,
            language=None, # Removed hardcoded "hi"
            beam_size=5,
            condition_on_previous_text=False
        )
        
        detected_language = info.language
        logger.info(f"🌐 Detected language: {detected_language} (Probability: {info.language_probability:.2f})")
        
        # Open file and ensure it writes to disk immediately
        with open(vtt_out, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
            for segment in segments:
                if hasattr(segment, 'words') and segment.words:
                    f.write(words_to_karaoke_vtt(segment.words, segment.start, segment.end))
                    f.flush() # Force write to internal buffer
                    os.fsync(f.fileno()) # Force write to disk
        
        logger.info(f"✅ Transcription completed")
        return detected_language
        
    except Exception as e:
        logger.error(f"❌ Transcription failed: {e}")
        raise
# ---------------- PROCESSING FUNCTIONS ----------------

def normalize_audio(src: Path, dst: Path) -> None:
    """Normalize audio to 16kHz mono WAV for transcription."""
    run_ffmpeg([
        "ffmpeg", "-y",
        "-i", str(src),
        "-ar", "16000",
        "-ac", "1",
        "-vn",
        str(dst)
    ], "Audio normalization")


def generate_audio_hls(src_audio: Path, out_dir: Path) -> None:
    """Generate multi-bitrate HLS streams in a single FFmpeg pass."""
    logger.info("📡 Generating multi-bitrate HLS streams...")
    
    # Ensure subdirectories exist
    for quality in HLS_QUALITIES.keys():
        (out_dir / quality).mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg", "-y", "-i", str(src_audio),
        # Quality 1: 64k
        "-map", "0:a", "-c:a:0", "aac", "-b:a:0", "64k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(out_dir / "64k/segment_%03d.ts"),
        str(out_dir / "64k/playlist.m3u8"),
        # Quality 2: 128k
        "-map", "0:a", "-c:a:1", "aac", "-b:a:1", "128k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(out_dir / "128k/segment_%03d.ts"),
        str(out_dir / "128k/playlist.m3u8"),
        # Quality 3: 256k
        "-map", "0:a", "-c:a:2", "aac", "-b:a:2", "256k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(out_dir / "256k/segment_%03d.ts"),
        str(out_dir / "256k/playlist.m3u8")
    ]
    
    run_ffmpeg(cmd, "Multi-bitrate HLS generation")


def write_master_m3u8(base_dir: Path, language: str = "en") -> None:
    """Creates the subtitle playlist and the master playlist."""
    
    # 1. Create the captions.m3u8 (The missing link)
    captions_dir = base_dir / "captions"
    captions_dir.mkdir(parents=True, exist_ok=True)
    captions_playlist = captions_dir / "playlist.m3u8"
    captions_playlist.write_text(f"""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:3600
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:3600.0,
captions.vtt
#EXT-X-ENDLIST""", encoding="utf-8")

    # 2. Update Master Playlist to point to the caption PLAYLIST, not the VTT
    lang_name = LANGUAGE_NAMES.get(language, language.upper())
    
    content = f"""#EXTM3U
#EXT-X-VERSION:3

#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="{lang_name}",LANGUAGE="{language}",DEFAULT=YES,AUTOSELECT=YES,URI="captions/playlist.m3u8"

#EXT-X-STREAM-INF:BANDWIDTH=64000,CODECS="mp4a.40.2",SUBTITLES="subs"
64k/playlist.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="mp4a.40.2",SUBTITLES="subs"
128k/playlist.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=256000,CODECS="mp4a.40.2",SUBTITLES="subs"
256k/playlist.m3u8
"""
    
    master_file = base_dir / "master.m3u8"
    master_file.write_text(content, encoding="utf-8")
    logger.info(f"✅ Master playlist created")


def upload_folder_to_s3(local_dir: Path, bucket: str, prefix: str) -> None:
    """Recursively upload folder to S3 with proper content types."""
    logger.info(f"☁️ Uploading to s3://{bucket}/{prefix}")
    
    uploaded_count = 0
    
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = Path(root) / file
            rel_path = full_path.relative_to(local_dir)
            s3_key = f"{prefix}/{rel_path.as_posix()}"

            content_type, _ = mimetypes.guess_type(str(full_path))
            extra_args = {}
            
            extension_map = {
                '.m3u8': 'application/vnd.apple.mpegurl',
                '.ts':   'video/mp2t',
                '.vtt':  'text/vtt'
            }

            ext = full_path.suffix.lower()
            if ext in extension_map:
                extra_args["ContentType"] = extension_map[ext]
            elif content_type:
                extra_args["ContentType"] = content_type

            try:
                s3.upload_file(str(full_path), bucket, s3_key, ExtraArgs=extra_args)
                uploaded_count += 1
            except ClientError as e:
                logger.error(f"❌ Failed to upload {s3_key}: {e}")
                raise
    
    logger.info(f"✅ Uploaded {uploaded_count} files to S3")


def cleanup_job(job_dir: Path, download_path: Path) -> None:
    """Clean up temporary files after processing."""
    try:
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)
            logger.info(f"🧹 Cleaned up work directory: {job_dir.name}")
            
        if download_path.exists():
            download_path.unlink()
            logger.info(f"🧹 Cleaned up download: {download_path.name}")
            
    except Exception as e:
        logger.warning(f"⚠️ Cleanup error: {e}")


def download_object(bucket: str, key: str, receipt_handle: str = None) -> None:
    """Main processing function for audio files."""
    decoded_key = urllib.parse.unquote_plus(key)
    original_path = Path(decoded_key)

    if not is_audio_file(decoded_key):
        logger.info(f"⏩ Skipping non-audio file: {decoded_key}")
        return

    safe_name = slugify(original_path.stem) + original_path.suffix
    if not safe_name or safe_name == original_path.suffix:
        safe_name = f"audio_{int(time.time())}{original_path.suffix}"

    download_path = DOWNLOAD_DIR / safe_name
    job_id = f"{slugify(original_path.stem)}_{int(time.time())}"
    job_dir = WORK_DIR / job_id
    
    logger.info(f"📦 Processing job: {job_id}")
    logger.info(f"   Bucket: {bucket}")
    logger.info(f"   Key: {decoded_key}")

    try:
        job_dir.mkdir(parents=True, exist_ok=True)
        
        # Download from S3
        logger.info(f"⬇️ Downloading from S3...")
        s3.download_file(bucket, decoded_key, str(download_path))
        
        # Immediate deletion after successful download
        if receipt_handle:
            try:
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                logger.info("✅ Message deleted immediately after download")
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete message early: {e}")
        
        # Check file size
        file_size_mb = download_path.stat().st_size / (1024 * 1024)
        logger.info(f"📊 File size: {file_size_mb:.2f} MB")
        
        if file_size_mb > MAX_FILE_SIZE_MB:
            raise ValueError(f"File too large: {file_size_mb:.2f}MB (max: {MAX_FILE_SIZE_MB}MB)")
        
        # Check disk space
        if not check_disk_space(file_size_mb):
            raise RuntimeError("Insufficient disk space")
        
        # Check duration
        duration = get_audio_duration(download_path)
        if duration > 0:
            logger.info(f"⏱️ Duration: {duration:.2f} seconds")
            if duration > MAX_DURATION_SECONDS:
                raise ValueError(f"Audio too long: {duration:.2f}s (max: {MAX_DURATION_SECONDS}s)")
        
        # Setup output structure
        song_name = slugify(original_path.stem)
        if not song_name:
            song_name = f"audio_{int(time.time())}"
            
        final_dir = job_dir / song_name
        original_dir = final_dir / "original"
        captions_dir = final_dir / "captions"

        original_dir.mkdir(parents=True, exist_ok=True)
        captions_dir.mkdir(parents=True, exist_ok=True)

        # Save original file
        shutil.copy(download_path, original_dir / safe_name)
        logger.info("✅ Original file saved")

        # Normalize audio for transcription
        normalized_audio = job_dir / "normalized.wav"
        logger.info("🎛️ Normalizing audio...")
        normalize_audio(download_path, normalized_audio)
        logger.info("✅ Audio normalized")

        # Generate captions (Default to Hindi 'hi' to avoid script confusion)
        captions_vtt = captions_dir / "captions.vtt"
        detected_language = generate_transcription(normalized_audio, captions_vtt, language="hi")

        # Generate HLS streams
        generate_audio_hls(download_path, final_dir)

        # Create master playlist
        write_master_m3u8(final_dir, language=detected_language)

        # Upload to production S3
        upload_folder_to_s3(final_dir, bucket=PRODUCTION_BUCKET_NAME, prefix=song_name)

        logger.info(f"🎉 Job completed successfully: {job_id}")
        logger.info(f"   Output: s3://{PRODUCTION_BUCKET_NAME}/{song_name}")

    except Exception as e:
        logger.error(f"❌ Processing failed for {job_id}: {e}")
        raise

    finally:
        cleanup_job(job_dir, download_path)
        logger.info("-" * 60)


# ---------------- SQS POLLING ----------------

def poll_sqs() -> None:
    """Continuously poll SQS queue for new audio processing jobs."""
    logger.info(f"🚀 SQS Poller started")
    logger.info(f"   Queue: {SQS_URL}")
    logger.info(f"   Source Bucket: {TEMP_BUCKET_NAME}")
    logger.info(f"   Destination Bucket: {PRODUCTION_BUCKET_NAME}")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                AttributeNames=['All']
            )

            messages = response.get('Messages', [])
            
            if not messages:
                continue

            logger.info(f"📬 Received {len(messages)} message(s)")

            for message in messages:
                receipt_handle = message['ReceiptHandle']
                
                try:
                    body = json.loads(message['Body'])
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Invalid JSON in message: {e}")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    continue

                # Handle S3 test events
                if body.get('Event') == 's3:TestEvent':
                    logger.info("🧪 Ignoring S3 Test Event")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    continue

                # Process S3 events
                try:
                    records = body.get('Records', [])
                    
                    if not records:
                        logger.warning("⚠️ No records in message body")
                        sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                        continue
                    
                    for record in records:
                        s3_info = record.get('s3', {})
                        bucket_info = s3_info.get('bucket', {})
                        object_info = s3_info.get('object', {})
                        
                        event_bucket = bucket_info.get('name')
                        object_key = object_info.get('key')
                        
                        if not object_key:
                            logger.warning("⚠️ No object key in S3 event")
                            continue
                        
                        logger.info("📥 New SQS job")
                        if event_bucket:
                            logger.debug(f"   Event bucket: {event_bucket}")
                        logger.info(f"   Key: {object_key}")
                        
                        # Always download from TEMP_BUCKET_NAME
                        download_object(TEMP_BUCKET_NAME, object_key, receipt_handle)

                    # Message deletion is now handled inside download_object

                except KeyError as e:
                    logger.error(f"❌ Missing expected field in message: {e}")
                    
                except Exception as e:
                    logger.error(f"❌ Job processing failed: {e}")
                    
                    # Re-queue the message to retry later (with delay to prevent tight loop)
                    try:
                        logger.info("This is the retry logic")  
                        sqs.send_message(
                            QueueUrl=SQS_URL,
                            MessageBody=message['Body'],
                            DelaySeconds=30  # Wait 30s before retrying
                        )
                        # Delete original message so it doesn't timeout and retry immediately
                        try:
                            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                        except Exception:
                            pass # Ignore if already deleted

                        logger.info("🔁 Message re-queued for retry")
                    except Exception as sqs_e:
                        logger.error(f"❌ Failed to re-queue message: {sqs_e}")

        except ClientError as e:
            logger.error(f"❌ SQS polling error: {e}")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Unexpected error in polling loop: {e}")
            time.sleep(5)


# ---------------- MAIN ----------------

def main():
    """Main entry point."""
    try:
        logger.info("=" * 60)
        logger.info("Audio Processing Service Starting")
        logger.info("=" * 60)
        
        # Verify FFmpeg is available
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            logger.info("✅ FFmpeg is available")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("❌ FFmpeg not found. Please install FFmpeg.")
            return
        
        if not shutil.which("ffprobe"):
            logger.error("❌ FFprobe not found. Please install FFmpeg.")
            return

        # Cleanup stale work artifacts on startup
        if WORK_DIR.exists():
            logger.info("🧹 Cleaning up stale work directory...")
            shutil.rmtree(WORK_DIR, ignore_errors=True)
            WORK_DIR.mkdir(exist_ok=True)
        
        # Start polling
        poll_sqs()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()