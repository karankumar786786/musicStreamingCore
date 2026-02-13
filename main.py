"""
Simplified Audio Processor - FIXED
- Multiple qualities (8k, 16k, 32k, 64k, 128k)
- No normalization
- No original file preservation
- Proper SQS message handling (no duplicates!)
- Simple folder structure
- URL decoding for S3 keys
"""

import os
import json
import time
import shutil
import subprocess
import logging
import urllib.parse
from pathlib import Path

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

if not all([SQS_URL, TEMP_BUCKET, PROD_BUCKET]):
    raise EnvironmentError("Missing required environment variables")

# Single work directory
WORK_DIR = Path("work")
WORK_DIR.mkdir(exist_ok=True)

AUDIO_EXTS = {".mp3", ".wav", ".aac", ".m4a", ".flac", ".ogg", ".opus"}

# Audio quality profiles
QUALITY_PROFILES = [
    {"bitrate": "32k", "bandwidth": 32000},
    {"bitrate": "64k", "bandwidth": 64000},
    {"bitrate": "128k", "bandwidth": 128000},
]

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
    """Convert seconds to VTT timestamp"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:06.3f}"


def transcribe_audio(audio_path, output_vtt):
    """Generate VTT transcript"""
    logger.info("🎙️ Transcribing...")
    
    segments, info = whisper_model.transcribe(
        str(audio_path),
        word_timestamps=False,
        language=None
    )
    
    logger.info(f"🌐 Detected: {info.language}")
    
    with open(output_vtt, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
        for segment in segments:
            start = format_timestamp(segment.start)
            end = format_timestamp(segment.end)
            text = segment.text.strip()
            f.write(f"{start} --> {end}\n{text}\n\n")
    
    logger.info("✅ Transcript complete")
    return info.language


def transcode_to_hls_multi_quality(input_audio, output_dir):
    """Transcode to HLS with multiple quality variants"""
    logger.info("🎬 Transcoding to HLS (multi-quality)...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Transcode each quality variant
    for profile in QUALITY_PROFILES:
        bitrate = profile["bitrate"]
        quality_dir = output_dir / bitrate
        quality_dir.mkdir(exist_ok=True)
        
        logger.info(f"   🔄 Encoding {bitrate}...")
        
        cmd = [
            "ffmpeg", "-y", "-i", str(input_audio),
            "-c:a", "aac", "-b:a", bitrate,
            "-f", "hls", "-hls_time", "6",
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", str(quality_dir / "segment_%03d.ts"),
            str(quality_dir / "playlist.m3u8")
        ]
        
        subprocess.run(cmd, check=True, capture_output=True)
    
    logger.info("✅ All qualities transcoded")


def create_master_playlist(output_dir, language="en"):
    """Create master.m3u8 with multiple quality variants"""
    
    # Build playlist content
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
    
    for profile in QUALITY_PROFILES:
        bitrate = profile["bitrate"]
        bandwidth = profile["bandwidth"]
        
        lines.append(f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},CODECS=\"mp4a.40.2\"")
        lines.append(f"{bitrate}/playlist.m3u8")
    
    content = "\n".join(lines) + "\n"
    
    (output_dir / "master.m3u8").write_text(content, encoding="utf-8")
    logger.info("✅ Master playlist created")


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
            
            # Skip the original audio file
            if file == 'audio.mp3':
                continue
            
            rel_path = file_path.relative_to(local_dir)
            s3_key = f"{prefix}/{rel_path.as_posix()}"
            
            ext = file_path.suffix.lower()
            extra_args = {}
            if ext in content_type_map:
                extra_args["ContentType"] = content_type_map[ext]
            
            s3.upload_file(str(file_path), bucket, s3_key, ExtraArgs=extra_args)
            count += 1
    
    logger.info(f"✅ Uploaded {count} files")


# ============= MAIN PROCESS =============

def process_audio(s3_key):
    """Main processing function"""
    
    timestamp = int(time.time())
    original_name = Path(s3_key).stem
    
    # ✅ Create a clean folder name
    song_name = original_name.replace(' ', '-').replace('_', '-')
    
    work_dir = WORK_DIR / f"{song_name}_{timestamp}"
    audio_file = work_dir / "audio.mp3"
    
    logger.info(f"📦 Processing: {s3_key}")
    logger.info(f"📁 Output folder: {song_name}")
    
    try:
        # Create work directory
        work_dir.mkdir(parents=True, exist_ok=True)
        
        # Download to work directory
        logger.info("⬇️ Downloading...")
        s3.download_file(TEMP_BUCKET, s3_key, str(audio_file))
        logger.info("✅ Downloaded")
        
        # Generate transcript
        vtt_path = work_dir / "captions.vtt"
        detected_lang = transcribe_audio(audio_file, vtt_path)
        
        # Transcode to HLS (multiple qualities)
        transcode_to_hls_multi_quality(audio_file, work_dir)
        
        # Create master playlist
        create_master_playlist(work_dir, language=detected_lang)
        
        # Upload to production
        upload_to_s3(work_dir, PROD_BUCKET, song_name)
        
        # Delete from temp bucket after success
        logger.info("🗑️ Deleting from temp bucket...")
        s3.delete_object(Bucket=TEMP_BUCKET, Key=s3_key)
        logger.info("✅ Deleted from temp")
        
        logger.info(f"🎉 SUCCESS: s3://{PROD_BUCKET}/{song_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # Cleanup work directory
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
            logger.info(f"🧹 Cleaned: {work_dir.name}")


# ============= SQS POLLING =============

def poll_sqs():
    """Poll SQS for jobs"""
    logger.info("🚀 SQS Poller started")
    logger.info(f"   Queue: {SQS_URL}")
    logger.info(f"   Temp Bucket: {TEMP_BUCKET}")
    logger.info(f"   Prod Bucket: {PROD_BUCKET}")
    logger.info(f"   Quality Profiles: {', '.join([p['bitrate'] for p in QUALITY_PROFILES])}")
    
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=1800
            )
            
            messages = response.get('Messages', [])
            if not messages:
                continue
            
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            
            try:
                body = json.loads(message['Body'])
                
                if body.get('Event') == 's3:TestEvent':
                    logger.info("🧪 Skipping test event")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    continue
                
                records = body.get('Records', [])
                if not records:
                    logger.warning("⚠️ No records")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    continue
                
                s3_key = records[0].get('s3', {}).get('object', {}).get('key')
                s3_key = urllib.parse.unquote_plus(s3_key)
                
                if not s3_key or not is_audio_file(s3_key):
                    logger.info(f"⏩ Skipping: {s3_key}")
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    continue
                
                logger.info(f"📥 Job: {s3_key}")
                
                success = process_audio(s3_key)
                
                if success:
                    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                    logger.info("✅ Message deleted (success)")
                else:
                    logger.warning("⚠️ Message NOT deleted (will retry)")
                
            except Exception as e:
                logger.error(f"❌ Processing error: {e}")
                logger.warning("⚠️ Message NOT deleted (will retry)")
        
        except Exception as e:
            logger.error(f"❌ Polling error: {e}")
            time.sleep(5)


# ============= MAIN =============

def main():
    """Entry point"""
    try:
        logger.info("=" * 60)
        logger.info("Audio Processor Starting")
        logger.info("=" * 60)
        
        # Verify FFmpeg
        try:
            subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            logger.info("✅ FFmpeg available")
        except:
            logger.error("❌ FFmpeg not found")
            return
        
        # Cleanup stale work
        if WORK_DIR.exists():
            logger.info("🧹 Cleaning stale work...")
            shutil.rmtree(WORK_DIR, ignore_errors=True)
            WORK_DIR.mkdir(exist_ok=True)
        
        # Start polling
        poll_sqs()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Shutdown")
    except Exception as e:
        logger.error(f"❌ Fatal: {e}")
        raise


if __name__ == "__main__":
    main()