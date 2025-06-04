import boto3
import json
import time
from datetime import datetime

# Configure boto3 client with custom endpoint
sqs = boto3.client(
    'sqs',
    endpoint_url='http://localhost:4566',
    region_name='us-east-1',  # Required but can be any valid region
    aws_access_key_id='sqslite-test-account',  # Dummy credentials for LocalStack
    aws_secret_access_key='test'
)

# Queue URL
queue_url = 'http://sqslite.local/sqslite-test-account/default'

def send_message_to_queue():
    """Send a message to the SQS queue"""
    try:
        # Create message payload
        message_body = {
            'timestamp': datetime.now().isoformat(),
            'message': f'Test message sent at {datetime.now()}',
            'counter': getattr(send_message_to_queue, 'counter', 0)
        }
        
        # Increment counter for next message
        send_message_to_queue.counter = getattr(send_message_to_queue, 'counter', 0) + 1
        
        # Send message to queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        
        print(f"Message sent successfully. MessageId: {response['MessageId']}")
        return True
        
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        return False

def main():
    print("Starting SQS message sender...")
    print(f"Queue URL: {queue_url}")
    print(f"Endpoint: http://localhost:4566")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            success = send_message_to_queue()
            if not(success):
                print("Waiting 10 seconds before retrying...\n")
                time.sleep(10)
                continue
            
    except KeyboardInterrupt:
        print("\nStopping message sender...")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()