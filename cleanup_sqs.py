import os
import sys
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

SQS_URL = os.getenv("SQS_URL")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

if not SQS_URL:
    print("❌ Error: SQS_URL not found in .env settings")
    sys.exit(1)

def cleanup_sqs():
    """Attempt to purge the queue completely, falling back to manual drain if needed."""
    
    # Initialize SQS client
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION)
    except Exception as e:
        print(f"❌ Failed to initialize SQS client: {e}")
        return

    print(f"🗑️ Attempting to clear SQS Queue: {SQS_URL}")
    print("--------------------------------------------------")

    # Try Purge (Fastest)
    try:
        print("⚡ Trying PurgeQueue...")
        sqs.purge_queue(QueueUrl=SQS_URL)
        print("✅ Success! Queue purged completely.")
        return

    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'PurgeQueueInProgress':
            print("⚠️ PurgeQueueInProgress: Wait 60 seconds between purges.")
        elif error_code == 'AWS.SimpleQueueService.PurgeQueueInProgress':
             print("⚠️ PurgeQueueInProgress: Wait 60 seconds between purges.")
        else:
            print(f"❌ PurgeQueue failed: {e}")
            
    except Exception as e:
        print(f"❌ unexpected error during purge: {e}")

    # Fallback to Manual Drain (Slower but reliable)
    print("\n🐢 Falling back to manual message deletion (drain loop)...")
    
    deleted_total = 0
    try:
        while True:
            # Receive up to 10 messages
            response = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1  # Short wait to check for messages
            )
            
            messages = response.get('Messages', [])
            
            if not messages:
                print("✅ Queue is empty (no more messages received).")
                break
                
            entries = []
            for msg in messages:
                entries.append({
                    'Id': msg['MessageId'],
                    'ReceiptHandle': msg['ReceiptHandle']
                })

            # Delete batch
            if entries:
                result = sqs.delete_message_batch(
                    QueueUrl=SQS_URL,
                    Entries=entries
                )
                
                successful = len(result.get('Successful', []))
                failed = len(result.get('Failed', []))
                
                deleted_total += successful
                print(f"   Deleted {successful} messages... (Total: {deleted_total})")
                
                if failed > 0:
                    print(f"   ⚠️ Failed to delete {failed} messages in this batch.")

    except Exception as e:
        print(f"❌ Error during manual drain: {e}")

    print("--------------------------------------------------")
    print(f"🏁 Cleanup finished. Total deleted manually: {deleted_total}")

if __name__ == "__main__":
    cleanup_sqs()
