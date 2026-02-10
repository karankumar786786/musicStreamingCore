"""
Simplified Audio Processing Pipeline (Local Whisper)
Flow: SQS -> Download -> Transcribe (Local Whisper) -> Transcode (128k) -> Upload -> Cleanup
"""

import os
import json
import time
import shutil
import subprocess
import logging
from pathlib import Path
import urllib.parse

import boto3
from faster_whisper import WhisperModel
from dotenv import load_dotenv

load_dotenv()

# ============= LOGGING =============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= CONFIG =============
SQS_URL = os.getenv("SQS_URL")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
TEMP_BUCKET = os.getenv("TEMP_BUCKET_NAME")
PROD_BUCKET = os.getenv("PRODUCTION_BUCKET_NAME")

# Validate
if not all([SQS_URL, TEMP_BUCKET, PROD_BUCKET]):
    raise EnvironmentError("Missing required environment variables")

# Directories
DOWNLOAD_DIR = Path("downloads")
WORK_DIR = Path("work")
DOWNLOAD_DIR.mkdir(exist_ok=True)
WORK_DIR.mkdir(exist_ok=True)

# Audio extensions
AUDIO_EXTS = {".mp3", ".wav", ".aac", ".m4a", ".flac", ".ogg", ".opus"}

# ============= WHISPER MODEL =============
logger.info("🤖 Loading Whisper model...")
whisper_model = WhisperModel("medium", device="cpu", compute_type="int8")
logger.info("✅ Whisper model loaded")

# ============= AWS CLIENTS =============
s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)
logger.info("✅ AWS clients initialized")


# ============= HELPER FUNCTIONS =============

def is_audio_file(filename):
    """Check if file is audio"""
    return any(filename.lower().endswith(ext) for ext in AUDIO_EXTS)


def format_timestamp(seconds):
    """Convert seconds to VTT timestamp (HH:MM:SS.mmm)"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:06.3f}"


def cleanup_local(download_path, work_path):
    """Clean up local files"""
    try:
        if download_path and download_path.exists():
            download_path.unlink()
            logger.info(f"🧹 Cleaned: {download_path.name}")
        
        if work_path and work_path.exists():
            shutil.rmtree(work_path, ignore_errors=True)
            logger.info(f"🧹 Cleaned: {work_path.name}")
    except Exception as e:
        logger.warning(f"⚠️ Cleanup warning: {e}")


# ============= TRANSCRIPTION =============

def transcribe_audio(audio_path, output_vtt):
    """Generate VTT transcript using local Whisper model"""
    logger.info("🎙️ Transcribing with local Whisper...")
    
    try:
        # Transcribe with auto language detection
        segments, info = whisper_model.transcribe(
            str(audio_path),
            word_timestamps=True,
            language=None  # Auto-detect language
        )
        
        detected_lang = info.language
        logger.info(f"🌐 Detected language: {detected_lang}")
        
        # Write VTT file
        with open(output_vtt, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
            
            for segment in segments:
                start = format_timestamp(segment.start)
                end = format_timestamp(segment.end)
                text = segment.text.strip()
                
                f.write(f"{start} --> {end}\n")
                f.write(f"{text}\n\n")
        
        logger.info(f"✅ Transcript saved: {output_vtt.name}")
        return detected_lang
        
    except Exception as e:
        logger.error(f"❌ Transcription failed: {e}")
        raise


# ============= TRANSCODING =============

def transcode_to_hls(input_audio, output_dir):
    """Transcode audio to HLS format (high quality 320k)"""
    logger.info("🎬 Transcoding to HLS (320k)...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "ffmpeg", "-y", "-i", str(input_audio),
        "-c:a", "aac", "-b:a", "320k",
        "-f", "hls", "-hls_time", "6",
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(output_dir / "segment_%03d.ts"),
        str(output_dir / "playlist.m3u8")
    ]
    
    subprocess.run(cmd, check=True, capture_output=True)
    logger.info("✅ Transcoding complete")


def create_master_playlist(output_dir, language="en"):
    """Create master.m3u8 playlist"""
    
    content = f"""#EXTM3U
#EXT-X-VERSION:3

#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Lyrics",LANGUAGE="{language}",DEFAULT=YES,AUTOSELECT=YES,URI="captions.vtt"

#EXT-X-STREAM-INF:BANDWIDTH=320000,CODECS="mp4a.40.2",SUBTITLES="subs"
playlist.m3u8
"""
    
    master_file = output_dir / "master.m3u8"
    master_file.write_text(content, encoding="utf-8")
    logger.info("✅ Master playlist created")


# ============= S3 UPLOAD =============

def upload_to_s3(local_dir, bucket, prefix):
    """Upload directory to S3"""
    logger.info(f"☁️ Uploading to s3://{bucket}/{prefix}")
    
    content_type_map = {
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts': 'video/mp2t',
        '.vtt': 'text/vtt'
    }
    
    count = 0
    for root, _, files in os.walk(local_dir):
        for file in files:
            file_path = Path(root) / file
            rel_path = file_path.relative_to(local_dir)
            s3_key = f"{prefix}/{rel_path.as_posix()}"
            
            ext = file_path.suffix.lower()
            extra_args = {}
            if ext in content_type_map:
                extra_args["ContentType"] = content_type_map[ext]
            
            s3.upload_file(str(file_path), bucket, s3_key, ExtraArgs=extra_args)
            count += 1
    
    logger.info(f"✅ Uploaded {count} files")


# ============= PROCESS AUDIO =============

def process_audio(s3_key):
    """Main processing function"""
    
    # Generate job ID
    filename = Path(s3_key).name
    job_id = f"{int(time.time())}_{filename}"
    
    download_path = DOWNLOAD_DIR / filename
    work_dir = WORK_DIR / job_id
    
    logger.info(f"📦 Processing: {s3_key}")
    
    try:
        # Download from temp bucket
        logger.info("⬇️ Downloading from S3...")
        s3.download_file(TEMP_BUCKET, s3_key, str(download_path))
        logger.info("✅ Downloaded")
        
        # Create work directory
        work_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate transcript
        vtt_path = work_dir / "captions.vtt"
        detected_lang = transcribe_audio(download_path, vtt_path)
        
        # Transcode to HLS
        transcode_to_hls(download_path, work_dir)
        
        # Create master playlist
        create_master_playlist(work_dir, language=detected_lang)
        
        # Upload to production bucket
        song_name = Path(s3_key).stem
        upload_to_s3(work_dir, PROD_BUCKET, song_name)
        
        # Delete from temp bucket after successful processing
        logger.info("🗑️ Deleting from temp bucket...")
        s3.delete_object(Bucket=TEMP_BUCKET, Key=s3_key)
        logger.info("✅ Deleted from temp bucket")
        
        logger.info(f"🎉 SUCCESS: s3://{PROD_BUCKET}/{song_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return False
        
    finally:
        # Always cleanup local files
        cleanup_local(download_path, work_dir)


# ============= SQS POLLING =============

def poll_sqs():
    """Poll SQS for new messages"""
    logger.info(f"🚀 Started polling SQS")
    logger.info(f"   Queue: {SQS_URL}")
    logger.info(f"   Temp Bucket: {TEMP_BUCKET}")
    logger.info(f"   Prod Bucket: {PROD_BUCKET}")
    
    while True:
        try:
            # Receive messages
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            messages = response.get('Messages', [])
            if not messages:
                continue
            
            for message in messages:
                receipt_handle = message['ReceiptHandle']
                
                try:
                    body = json.loads(message['Body'])
                    
                    # Ignore test events
                    if body.get('Event') == 's3:TestEvent':
                        logger.info("🧪 Ignoring test event")
                        sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                        continue
                    
                    # Process S3 events
                    records = body.get('Records', [])
                    if not records:
                        logger.warning("⚠️ No records in message")
                        sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                        continue
                    
                    for record in records:
                        s3_key_raw = record.get('s3', {}).get('object', {}).get('key')
                        
                        if not s3_key_raw:
                            logger.warning("⚠️ No S3 key found")
                            continue

                        # Decode key (S3 events are URL encoded)
                        s3_key = urllib.parse.unquote_plus(s3_key_raw)
                        logger.info(f"🔑 Key Raw: {s3_key_raw}")
                        logger.info(f"🔑 Key Decoded: {s3_key}")
                        
                        # Check if audio file
                        if not is_audio_file(s3_key):
                            logger.info(f"⏩ Skipping non-audio: {s3_key}")
                            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                            continue
                        
                        # Process the audio
                        success = process_audio(s3_key)
                        
                        if success:
                            # Delete message from SQS
                            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                            logger.info("✅ Message deleted from SQS")
                        else:
                            # Re-queue with delay for retry
                            logger.info("🔁 Re-queuing message for retry...")
                            sqs.send_message(
                                QueueUrl=SQS_URL,
                                MessageBody=message['Body'],
                                DelaySeconds=60
                            )
                            # Delete original to avoid duplicate
                            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                            logger.info("✅ Message re-queued")
                
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Invalid JSON: {e}")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    
                except Exception as e:
                    logger.error(f"❌ Message processing error: {e}")
                    # Let message timeout and retry automatically
        
        except Exception as e:
            logger.error(f"❌ Polling error: {e}")
            time.sleep(5)


# ============= MAIN =============

def main():
    """Entry point"""
    try:
        logger.info("=" * 60)
        logger.info("AUDIO PROCESSING SERVICE (Local Whisper)")
        logger.info("=" * 60)
        
        # Check FFmpeg
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            logger.info("✅ FFmpeg available")
        except:
            logger.error("❌ FFmpeg not found")
            return
        
        # Start polling
        poll_sqs()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()