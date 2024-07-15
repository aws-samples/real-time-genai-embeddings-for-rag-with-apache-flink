import os
import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_opensearch_client(host, region='us-east-1'):
    session = boto3.Session()
    credentials = session.get_credentials()
    auth = AWSV4SignerAuth(credentials, region, 'aoss')

    # Initialize OpenSearch client with AWS credentials
    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        region=region,
        connection_class=RequestsHttpConnection
    )
    return client

def index_exists(client, index_name):
    try:
        # Check if the index exists
        return client.indices.exists(index=index_name)
    except Exception as e:
        logger.error(f"Error checking if index '{index_name}' exists: {str(e)}")
        return False

def create_index(client, index_name, index_body):
    try:
        # Check if the index already exists
        if index_exists(client, index_name):
            logger.info(f"Index '{index_name}' already exists.")
            return

        # If the index does not exist, create it
        response = client.indices.create(index=index_name, body=index_body)
        if response.get('acknowledged', False):
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.error(f"Failed to create index '{index_name}'. Response: {response}")
    except Exception as e:
        logger.error(f"Exception occurred while creating index '{index_name}': {str(e)}")

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    # Retrieve environment variables
    host = os.environ['aosDomain']
    embeddingModel = os.environ['embeddingModel']

    if embeddingModel=='titan-v1':
        dimension=1536
    elif embeddingModel=='titan-v2':
        dimension=1024

    index_name = 'msk-flink-aoss-rag-index'

    # Define the index body
    index_body = {
        "mappings": {
            "properties": {
                "passage_embedding": {
                    "type": "knn_vector",
                    "dimension": dimension,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                "date": {
                    "type": "date"
                },
                "text": {
                    "type": "text"
                }
            }
        },
        "settings": {
            "index": {
                "knn": True,
                "number_of_shards": 5,
                "number_of_replicas": 1
            }
        }
    }

    # Remove 'https://' prefix from host if present
    if host.startswith('https://'):
        host = host[len('https://'):]

    request_type = event['RequestType']
    match request_type:
        case 'Update':
            print('Update of stack')
            # Initialize OpenSearch client
            client = create_opensearch_client(host)

            # Create or check if index exists
            create_index(client, index_name, index_body)

            # Example response
            return {
                'statusCode': 200,
                'body': json.dumps('Index creation process completed')
            }
        case 'Delete':
            print('Delete Function')
        case 'Create':

            # Initialize OpenSearch client
            client = create_opensearch_client(host)

            # Create or check if index exists
            create_index(client, index_name, index_body)

            # Example response
            return {
                'statusCode': 200,
                'body': json.dumps('Index creation process completed')
            }
