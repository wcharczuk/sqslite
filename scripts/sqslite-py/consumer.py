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

def consume_messages():
    """Consume messages from the SQS queue"""
    try:
        # Receive messages from queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Receive up to 10 messages at once
            WaitTimeSeconds=20,      # Long polling - wait up to 20 seconds for messages
            VisibilityTimeout=30     # Hide message from other consumers for 30 seconds
        )
        
        # Check if any messages were received
        if 'Messages' not in response:
            print("No messages available in queue")
            return 0
        
        messages = response['Messages']
        print(f"Received {len(messages)} message(s)")
        
        # Process each message
        for message in messages:
            process_message(message)
            
        return len(messages)
        
    except Exception as e:
        print(f"Error receiving messages: {str(e)}")
        return 0

def process_message(message):
    """Process and delete a single message"""
    try:
        # Extract message details
        message_id = message['MessageId']
        receipt_handle = message['ReceiptHandle']
        body = message['Body']
        
        print(f"\n--- Processing Message ---")
        print(f"MessageId: {message_id}")
        print(f"Body: {body}")
        
        # Try to parse JSON body
        try:
            parsed_body = json.loads(body)
            print(f"Parsed content: {json.dumps(parsed_body, indent=2)}")
        except json.JSONDecodeError:
            print("Message body is not valid JSON")
        
        # Simulate processing time
        print("Processing message...")
        time.sleep(0.1)
        
        # Delete message from queue after successful processing
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        print(f"Message {message_id} processed and deleted successfully")
        
    except Exception as e:
        print(f"Error processing message {message.get('MessageId', 'unknown')}: {str(e)}")

def main():
    print("Starting SQS message consumer...")
    print(f"Queue URL: {queue_url}")
    print(f"Endpoint: http://localhost:4566")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        while True:
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Polling for messages...")
            
            messages_received = consume_messages()
            message_count += messages_received
            
            if messages_received > 0:
                print(f"Total messages processed so far: {message_count}")
            else:
                print("Waiting for messages...")
            
            # Short pause between polling cycles when no messages
            if messages_received == 0:
                time.sleep(2)
                
    except KeyboardInterrupt:
        print(f"\nStopping message consumer...")
        print(f"Total messages processed: {message_count}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()