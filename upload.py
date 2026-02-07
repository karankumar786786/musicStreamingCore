import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Client - Uses environment variables
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
)

# Use bucket from .env
BUCKET = os.getenv("TEMP_BUCKET_NAME", "musicstreaming-uploads")

# File to upload
FILE_NAME = "Ek Mulakat Zaruri Hai Sanam (( Sad😰 💔 Song ))HD Sirf Tum 1999 _ Jaspinder Narula, Sabri Brothers.mp3"
FILE_PATH = Path(__file__).parent / FILE_NAME

def upload():
    if not FILE_PATH.exists():
        print(f"❌ File not found: {FILE_PATH}")
        return

    print(f"⬆️ Uploading {FILE_PATH.name} to {BUCKET}...")
    try:
        s3.upload_file(str(FILE_PATH), BUCKET, FILE_PATH.name)
        print("✅ Upload complete!")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    upload()
