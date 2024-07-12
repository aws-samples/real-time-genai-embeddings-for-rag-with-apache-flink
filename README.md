# Real Time Gen AI Embeddings for RAG With Amazon Bedrock and Apache Flink

In this code repository you will find multiple Streaming RAG architectures that use Amazon Bedrock Embeddings Models and Apache Flink to process, embed and ingest into vector databases

Each sample architecture has its own CDK Deployment

## Streaming Gen AI Architectures

For sources you have:

- Amazon Kinesis Data Streams
- Amazon MSK Provisioned

For Vector Databases you have: 

- Amazon OpenSearch Serverless
- Amazon OpenSearch Provisioned

We will add more Vector Databases support in the future

For Embeddings models you can choose from:

- Amazon Titan V1 on Amazon Bedrock (1536 Dimensions)
- Amazon Titan V2 on Amazon Bedrock (1024 Dimensions)

## Deployment
Each Sample will deploy the following:
- Streaming Source (Amazon Kinesis Data Streams or Amazon MSK)
- Amazon Managed Service for Apache Flink Application
- Amazon Opensearch Vector Database
- AWS Lambda functions for
  - Creating OpenSearch Index
  - Starting Flink Application
  - Stream messages producer

In every sample the steps to deploy are the following:

- Go into the directory of the Streaming RAG GenAI solution you want to deploy
- Install node modules *npm install*
- Go into flink-bedrock directory and build the Apache Flink Application *mvn clean package*
- Go back to the Streaming RAG GenAI Solution directory
- Boostrap your environment *cdk bootstrap*
- Deploy the solution, specifying which embedding model you want to use: *cdk deploy --parameters EmbeddingModel=titan-v2*

You can set
- titan-v1
- titan-v2


##
TODO: Fill this README out!

Be sure to:

* Change the title in this README
* Edit your repository description on GitHub

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

