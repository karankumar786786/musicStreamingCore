import os
import boto3
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_sqs_connectivity():
    sqs_url = os.getenv("SQS_URL")
    region = os.getenv("AWS_REGION", "ap-south-1")
    
    print(f"🔍 Testing SQS connectivity to: {sqs_url}")
    
    try:
        sqs = boto3.client(
            'sqs', 
            region_name=region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        
        # Try to get queue attributes to verify permissions
        response = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=['QueueArn']
        )
        
        queue_arn = response.get('Attributes', {}).get('QueueArn')
        print(f"✅ Successfully connected to SQS! Queue ARN: {queue_arn}")
        
    except Exception as e:
        print(f"❌ Failed to connect to SQS: {e}")

if __name__ == "__main__":
    test_sqs_connectivity()
