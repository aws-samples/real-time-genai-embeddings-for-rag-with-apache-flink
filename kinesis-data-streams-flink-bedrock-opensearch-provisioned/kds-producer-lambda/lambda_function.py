import json
import boto3
from datetime import datetime
import os

def lambda_handler(event, context):
    # Extract event data
    kds_stream_name = os.environ['kds']

    text = event.get('text')

    # Generate timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Construct the JSON payload
    payload = {
        "text": text,
        "created_at": timestamp
    }

    try:
        # Initialize Kinesis client
        kinesis_client = boto3.client('kinesis')

        # Convert payload to JSON string
        payload_json = json.dumps(payload)

        # Put record into Kinesis Data Stream
        response = kinesis_client.put_record(
            StreamName=kds_stream_name,
            Data=payload_json,
            PartitionKey='default'
        )

        print(f"Successfully sent message to Kinesis Data Stream '{kds_stream_name}'")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Successfully sent message to Kinesis Data Stream'}),
        }

    except Exception as e:
        print(f"Failed to send message to Kinesis Data Stream '{kds_stream_name}': {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Failed to send message to Kinesis Data Stream: {str(e)}'}),
        }
