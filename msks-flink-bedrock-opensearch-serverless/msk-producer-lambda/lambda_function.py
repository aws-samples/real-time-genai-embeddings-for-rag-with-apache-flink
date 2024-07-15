import json
import os
import logging
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MSKTokenProvider():
    def __init__(self, region):
        self.region = region

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token

def lambda_handler(event, context):
    # Extract environment variables
    bootstrap_servers = os.environ['BOOTSTRAP_SERVERS']
    region = os.environ['region']
    topic_name = os.environ['TOPIC_NAME']

    text = event.get('text')

    # Generate timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Construct the JSON payload
    payload = {
        "text": text,
        "created_at": timestamp
    }

    try:
        # Initialize Kafka producer
        tp = MSKTokenProvider(region=region)

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=tp,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id=socket.gethostname()
        )

        # Send message to Kafka topic
        future = producer.send(topic_name, value=payload)

        # Wait for send to complete
        result = future.get(timeout=10)

        logger.info(f"Successfully sent message to Kafka topic '{topic_name}'")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Successfully sent message to Kafka topic'}),
        }

    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka topic '{topic_name}': {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Failed to send message to Kafka topic: {str(e)}'}),
        }

    finally:
        producer.close()
