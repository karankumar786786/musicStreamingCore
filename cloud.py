"""
Simplified Audio Processing Pipeline
Flow: SQS -> Download -> Transcribe (HF API) -> Transcode (128k) -> Upload -> Cleanup
"""

import os
import json
import time
import shutil
import subprocess
import logging
from pathlib import Path
import requests
import urllib.parse

import boto3
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
HF_API_KEY = os.getenv("HUGGINGFACE_API_KEY")

# Validate
if not all([SQS_URL, TEMP_BUCKET, PROD_BUCKET, HF_API_KEY]):
    raise EnvironmentError("Missing required environment variables")

# Directories
DOWNLOAD_DIR = Path("downloads")
WORK_DIR = Path("work")
DOWNLOAD_DIR.mkdir(exist_ok=True)
WORK_DIR.mkdir(exist_ok=True)

# Audio extensions
AUDIO_EXTS = {".mp3", ".wav", ".aac", ".m4a", ".flac", ".ogg", ".opus"}

# Hugging Face API
HF_API_URL = "https://api-inference.huggingface.co/models/openai/whisper-large-v3"
HF_HEADERS = {"Authorization": f"Bearer {HF_API_KEY}"}

# ============= AWS CLIENTS =============
s3 = boto3.client('s3', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)
logger.info("✅ AWS clients initialized")


# ============= HELPER FUNCTIONS =============

def is_audio_file(filename):
    """Check if file is audio"""
    return any(filename.lower().endswith(ext) for ext in AUDIO_EXTS)


def check_disk_space(required_mb):
    """Check if sufficient disk space is available"""
    stat = shutil.disk_usage(WORK_DIR)
    available_mb = stat.free / (1024 * 1024)
    
    if available_mb < required_mb * 2:
        logger.warning(f"⚠️ Low disk space: {available_mb:.2f}MB available, {required_mb*2:.2f}MB required")
        return False
    return True


def get_audio_duration(audio_path):
    """Get audio duration in seconds using ffprobe"""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(audio_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(result.stdout.strip())
    except Exception as e:
        logger.warning(f"⚠️ Could not get audio duration: {e}")
        return 0.0


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

def transcribe_with_huggingface(audio_path, output_vtt):
    """Generate VTT transcript using Hugging Face Inference API"""
    logger.info("🎙️ Transcribing with Hugging Face API...")
    
    try:
        # Read audio file
        with open(audio_path, "rb") as f:
            audio_data = f.read()
        
        # Call Hugging Face API
        response = requests.post(
            HF_API_URL,
            headers=HF_HEADERS,
            data=audio_data,
            timeout=300  # 5 minutes timeout
        )
        
        if response.status_code != 200:
            raise Exception(f"HF API error: {response.status_code} - {response.text}")
        
        result = response.json()
        
        # Write VTT file
        with open(output_vtt, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
            
            # Check if response has chunks (word-level timestamps)
            if "chunks" in result:
                for chunk in result["chunks"]:
                    start = chunk["timestamp"][0]
                    end = chunk["timestamp"][1]
                    text = chunk["text"].strip()
                    
                    f.write(f"{format_timestamp(start)} --> {format_timestamp(end)}\n")
                    f.write(f"{text}\n\n")
            else:
                # Fallback: single segment
                text = result.get("text", "")
                f.write(f"00:00:00.000 --> 00:10:00.000\n")
                f.write(f"{text}\n\n")
        
        logger.info(f"✅ Transcript saved: {output_vtt.name}")
        
    except Exception as e:
        logger.error(f"❌ Transcription failed: {e}")
        raise


# ============= TRANSCODING =============

def transcode_to_hls(input_audio, output_dir):
    """Transcode audio to HLS format (multi-bitrate: 64k, 128k, 256k)"""
    logger.info("🎬 Transcoding to HLS (64k, 128k, 256k)...")
    
    # Create subdirectories
    for bitrate in ["64k", "128k", "256k"]:
        (output_dir / bitrate).mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "ffmpeg", "-y", "-i", str(input_audio),
        # Quality 1: 64k
        "-map", "0:a", "-c:a:0", "aac", "-b:a:0", "64k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(output_dir / "64k/segment_%03d.ts"),
        str(output_dir / "64k/playlist.m3u8"),
        # Quality 2: 128k
        "-map", "0:a", "-c:a:1", "aac", "-b:a:1", "128k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(output_dir / "128k/segment_%03d.ts"),
        str(output_dir / "128k/playlist.m3u8"),
        # Quality 3: 256k
        "-map", "0:a", "-c:a:2", "aac", "-b:a:2", "256k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(output_dir / "256k/segment_%03d.ts"),
        str(output_dir / "256k/playlist.m3u8")
    ]
    
    subprocess.run(cmd, check=True, capture_output=True)
    logger.info("✅ Transcoding complete")


def create_master_playlist(output_dir, has_captions=True):
    """Create master.m3u8 playlist"""
    
    # 1. Create captions playlist if needed
    if has_captions:
        captions_dir = output_dir / "captions"
        captions_dir.mkdir(parents=True, exist_ok=True)
        captions_playlist = captions_dir / "playlist.m3u8"
        captions_playlist.write_text("""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:3600
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:3600.0,
../captions.vtt
#EXT-X-ENDLIST""", encoding="utf-8")

        # Move vtt file to fit the referencing structure if needed, 
        # but here we referenced "../captions.vtt" assuming it's in the root of work_dir
        # Actually main.py puts captions.vtt inside captions dir. Let's align.
        # However, in process_audio, we write to work_dir/captions.vtt directly.
        # Let's move it to captions/captions.vtt to match main.py structure
        
        src_vtt = output_dir / "captions.vtt"
        dst_vtt = captions_dir / "captions.vtt"
        if src_vtt.exists():
            shutil.move(src_vtt, dst_vtt)
            
        # Update playlist to point to local vtt
        captions_playlist.write_text("""#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:3600
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
#EXTINF:3600.0,
captions.vtt
#EXT-X-ENDLIST""", encoding="utf-8")


    # 2. Create Master Playlist
    content = """#EXTM3U
#EXT-X-VERSION:3

#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",LANGUAGE="en",DEFAULT=YES,AUTOSELECT=YES,URI="captions/playlist.m3u8"

#EXT-X-STREAM-INF:BANDWIDTH=64000,CODECS="mp4a.40.2",SUBTITLES="subs"
64k/playlist.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="mp4a.40.2",SUBTITLES="subs"
128k/playlist.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=256000,CODECS="mp4a.40.2",SUBTITLES="subs"
256k/playlist.m3u8
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
        
        # Check file size
        file_size_mb = download_path.stat().st_size / (1024 * 1024)
        logger.info(f"📊 File size: {file_size_mb:.2f} MB")
        
        if file_size_mb > 500: # 500MB limit
             raise ValueError(f"File too large: {file_size_mb:.2f}MB")
             
        # Check disk space
        if not check_disk_space(file_size_mb):
            raise RuntimeError("Insufficient disk space")
            
        # Check duration
        duration = get_audio_duration(download_path)
        if duration > 0:
            logger.info(f"⏱️ Duration: {duration:.2f} seconds")
            if duration > 3600: # 1 hour limit
                raise ValueError(f"Audio too long: {duration:.2f}s")
        
        # Create work directory
        work_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate transcript
        vtt_path = work_dir / "captions.vtt"
        transcribe_with_huggingface(download_path, vtt_path)
        
        # Transcode to HLS
        transcode_to_hls(download_path, work_dir)
        
        # Create master playlist
        create_master_playlist(work_dir, has_captions=True)
        
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
        logger.info("AUDIO PROCESSING SERVICE")
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