import os
import json
import subprocess
import urllib.parse
from pathlib import Path
import mimetypes
import shutil

import boto3
from botocore.config import Config
from fastapi import FastAPI, Request, BackgroundTasks

# -------------------- CONFIG --------------------

PORT = 3000

BASE_DIR = Path(__file__).parent
DOWNLOAD_DIR = BASE_DIR / "rustfs-downloads"
WORK_DIR = BASE_DIR / "work"
DOWNLOAD_DIR.mkdir(exist_ok=True)
WORK_DIR.mkdir(exist_ok=True)

UPLOAD_BUCKET = "uploads"
PROD_BUCKET = "production"

AUDIO_EXT = {".mp3", ".wav", ".aac", ".m4a", ".flac"}

# -------------------- APP --------------------

app = FastAPI()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("RUSTFS_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id="rustfsadmin",
    aws_secret_access_key="ChangeMe123!",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

# -------------------- HELPERS --------------------

def is_audio_file(path: Path) -> bool:
    return path.suffix.lower() in AUDIO_EXT


def run(cmd: list):
    subprocess.run(cmd, check=True)


def normalize_audio(src: Path, dst: Path):
    run([
        "ffmpeg", "-y",
        "-i", str(src),
        "-ar", "16000",
        "-ac", "1",
        str(dst)
    ])


def generate_captions(audio: Path, vtt_out: Path):
    """
    Uses faster-whisper.
    Replace model size as needed.
    """
    from faster_whisper import WhisperModel

    model = WhisperModel("medium", device="cpu", compute_type="int8")
    segments, info = model.transcribe(str(audio))
    
    language = info.language
    print(f"🌐 Detected language: {language}")

    with open(vtt_out, "w") as f:
        f.write("WEBVTT\n\n")
        for s in segments:
            start = format_ts(s.start)
            end = format_ts(s.end)
            f.write(f"{start} --> {end}\n{s.text.strip()}\n\n")
    
    return language


def format_ts(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return f"{h:02}:{m:02}:{s:06.3f}"


def generate_hls(audio: Path, hls_dir: Path):
    (hls_dir / "64k").mkdir(parents=True)
    (hls_dir / "128k").mkdir(parents=True)

    run([
        "ffmpeg", "-y",
        "-i", str(audio),

        "-map", "0:a", "-c:a", "aac", "-b:a", "64k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(hls_dir / "64k/seg_%03d.ts"),
        str(hls_dir / "64k/playlist.m3u8"),

        "-map", "0:a", "-c:a", "aac", "-b:a", "128k",
        "-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(hls_dir / "128k/seg_%03d.ts"),
        str(hls_dir / "128k/playlist.m3u8"),
    ])


def write_master_m3u8(hls_dir: Path, language: str = "en"):
    content = f"""#EXTM3U
#EXT-X-VERSION:3

#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="{language.upper()}",
DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="{language}",
URI="captions/captions.vtt"

#EXT-X-STREAM-INF:BANDWIDTH=64000,CODECS="mp4a.40.2",SUBTITLES="subs"
64k/playlist.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="mp4a.40.2",SUBTITLES="subs"
128k/playlist.m3u8
"""
    (hls_dir / "master.m3u8").write_text(content)


def upload_folder(local_dir: Path, s3_prefix: str):
    for root, _, files in os.walk(local_dir):
        for file in files:
            full = Path(root) / file
            rel = full.relative_to(local_dir)
            key = f"{s3_prefix}/{rel}"

            s3.upload_file(
                str(full),
                PROD_BUCKET,
                key,
                ExtraArgs={"ContentType": mimetypes.guess_type(file)[0] or "application/octet-stream"}
            )

# -------------------- PIPELINE --------------------

def process_audio(bucket: str, key: str):
    decoded_key = urllib.parse.unquote_plus(key)
    filename = Path(decoded_key).name
    song_name = Path(filename).stem

    local_audio = DOWNLOAD_DIR / filename
    work_song = WORK_DIR / song_name

    original_dir = work_song / "original"
    hls_dir = work_song / "hls"
    captions_dir = hls_dir / "captions"

    original_dir.mkdir(parents=True, exist_ok=True)
    captions_dir.mkdir(parents=True, exist_ok=True)

    print(f"⬇️ Downloading {decoded_key}")
    s3.download_file(bucket, decoded_key, str(local_audio))

    if not is_audio_file(local_audio):
        print("❌ Not an audio file. Skipping.")
        return

    normalized = work_song / "normalized.wav"
    normalize_audio(local_audio, normalized)

    captions_vtt = captions_dir / "captions.vtt"
    lang = generate_captions(normalized, captions_vtt)

    generate_hls(normalized, hls_dir)
    write_master_m3u8(hls_dir, lang)

    # save original
    os.rename(local_audio, original_dir / filename)

    upload_folder(work_song, song_name)

    print(f"🧹 Cleaning up local files for: {song_name}")
    try:
        shutil.rmtree(work_song)
        if local_audio.exists():
            local_audio.unlink()
    except Exception as e:
        print(f"⚠️ Cleanup error: {e}")

    print(f"✅ Processed & uploaded: {song_name}")

# -------------------- WEBHOOK --------------------

@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()

    for r in data.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]

        background_tasks.add_task(process_audio, bucket, key)

    return {"status": "ok"}

# -------------------- RUN --------------------

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 Webhook running on http://localhost:{PORT}/webhook")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
