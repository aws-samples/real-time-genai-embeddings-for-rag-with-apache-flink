import boto3
import logging
import os
import json
import time
import socket
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MSKTokenProvider():
    def __init__(self, region):
        self.region = region

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token

def on_event(event, context):
    logger.info(event)
    request_type = event['RequestType']
    match request_type:
        case 'Create':
            return create_topic(event)
        case 'Update':
            print('Update')
        case 'Delete':
            print('Delete')
        case _:
            logger.error(f'Unexpected RequestType: {event["RequestType"]}')

    return

def create_topic(event):
    logger.info('Creating Topic')

    region = event['ResourceProperties']['region']
    bootstrap_servers = event['ResourceProperties']['bootstrap_servers']
    topic_name = event['ResourceProperties']['topic_name']
    num_partitions = int(event['ResourceProperties'].get('num_partitions', 1))
    replication_factor = int(event['ResourceProperties'].get('replication_factor', 1))

    try:
        tp = MSKTokenProvider(region=region)

        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_SSL',
            sasl_mechanism='OAUTHBEARER',
            sasl_oauth_token_provider=tp,
            client_id=socket.gethostname(),
        )

        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()

        logger.info(f"Topic '{topic_name}' created successfully.")
        return {
            'Status': 'SUCCESS',
            'Data': {
                'Message': f"Topic '{topic_name}' created successfully."
            }
        }
    except Exception as e:
        logger.error(f"Failed to create topic: {str(e)}")
        return {
            'Status': 'FAILED',
            'Data': {
                'Message': f"Failed to create topic: {str(e)}"
            }
        }
