import * as cdk from 'aws-cdk-lib';
import {aws_logs, CfnParameter, CustomResource, RemovalPolicy} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as opensearchserverless from 'aws-cdk-lib/aws-opensearchserverless';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2'
import * as assets from 'aws-cdk-lib/aws-s3-assets'
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {Provider} from "aws-cdk-lib/custom-resources";


export class KinesisDataStreamsFlinkBedrockOpensearchServerless extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const EmbeddingModel = new CfnParameter(this, "EmbeddingModel", {
      type: "String",
      description: "Which Amazon Bedrock Model use for Embeddings (Titan V1 , Titan V2"});


    //Jar Connectors
    const flinkApplicationJar = new assets.Asset(this, 'flinkApplicationJar', {
      path: ('./flink-bedrock/target/flink-bedrock-1.0-SNAPSHOT.jar'),
    });

    const collectionName = 'kds-vector-db-cdk'; // Example collection name
    // Define CFN parameters
    const s3Bucket = new cdk.CfnParameter(this, 's3BucketARN', {
      type: 'String',
      description: 'Description of parameter 1',
      default: 'Default value',
    });

    const s3FileKey = new cdk.CfnParameter(this, 's3FileKey', {
      type: 'String',
      description: 'Description of parameter 1',
      default: 'Default value',
    });
    // Network Security Policy
    // Network Security Policy
    const networkSecurityPolicy = [
      {
        "Rules": [
          {
            "Resource": [
              "collection/"+collectionName
            ],
            "ResourceType": "dashboard"
          },
          {
            "Resource": [
              "collection/"+collectionName
            ],
            "ResourceType": "collection"
          }
        ],
        "AllowFromPublic": true
      }
    ]



    const networkSecurityPolicyName = `${collectionName}-network-policy`;
    // Create network security policy
    const cfnNetworkSecurityPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'NetworkSecurityPolicy', {
      policy: JSON.stringify(networkSecurityPolicy),
      name: networkSecurityPolicyName,
      type: 'network',
    });

    const encryptionSecurityPolicy =
      {
        "Rules": [
          {
            "Resource": [
              "collection/"+collectionName
            ],
            "ResourceType": "collection"
          }
        ],
        "AWSOwnedKey": true
      }


    const encryptionSecurityPolicyName = `${collectionName}-encryption-pl`;
    // Create network security policy
    const cfnEncryptionSecurityPolicy = new opensearchserverless.CfnSecurityPolicy(this, 'EncryptionSecurityPolicy', {
      policy: JSON.stringify(encryptionSecurityPolicy),
      name: encryptionSecurityPolicyName,
      type: 'encryption',
    });

    // our KDA app needs access to describe kinesisanalytics
    const kdaAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:kinesisanalytics:"+this.region+":"+this.account+":application/kds-msf-aoss"],
          actions: ['kinesisanalyticsv2:DescribeApplication','kinesisAnalyticsv2:UpdateApplication']
        }),
      ],
    });

    // Create a Kinesis Data Stream
    const kinesisStream = new kinesis.Stream(this, 'MyKinesisStream', {
      streamName: "kds-aoss-source-rag",
      shardCount: 1, // You can adjust the number of shards based on your requirements,
    });

    // our KDA app needs to be able to log
    const kinesisPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [kinesisStream.streamArn],
          actions: ["kinesis:DescribeStream",
            "kinesis:GetShardIterator",
            "kinesis:GetRecords",
            "kinesis:PutRecord",
            "kinesis:PutRecords",
            "kinesis:ListShards"
          ]
        }),
      ],
    });

    const bedRockPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          resources: ["arn:aws:bedrock:"+this.region+"::foundation-model/cohere.embed-english-v3","arn:aws:bedrock:"+this.region+"::foundation-model/cohere.embed-multilingual-v3","arn:aws:bedrock:"+this.region+"::foundation-model/amazon.titan-embed-text-v1","arn:aws:bedrock:"+this.region+"::foundation-model/amazon.titan-embed-text-v2:0"],
          actions: ["bedrock:GetFoundationModel","bedrock:InvokeModel"]
        })
      ],
    });

    const logGroup = new logs.LogGroup(this, 'MyLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      logGroupName: this.stackName+"-log-group",// Adjust retention as needed
      removalPolicy: RemovalPolicy.DESTROY
    });

    const logStream = new logs.LogStream(this, 'MyLogStream', {
      logGroup,
    });

    const accessCWLogsPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName,
            "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName+":log-stream:" + logStream.logStreamName],
          actions: ['logs:PutLogEvents','logs:DescribeLogGroups','logs:DescribeLogStreams','cloudwatch:PutMetricData'],
        }),
      ],
    });


    const s3Policy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['s3:*']
        }),
      ],
    });

    const aossPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['aoss:*']
        }),
      ],
    });




    const cfnCollection = new opensearchserverless.CfnCollection(this, 'OpssSearchCollection', {
      name: collectionName,
      description: 'Collection to be used for search using OpenSearch Serverless',
      type: 'VECTORSEARCH', // [SEARCH, TIMESERIES, VECTORSEARCH]
    });




    const LambdaBedRockRole = new iam.Role(this, 'Lambda BedRock Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: this.stackName+"bedrock-role",
      description: 'BedRock Role',
      inlinePolicies: {
        BedRockRole: bedRockPolicy,
        KinesisPolicy: kinesisPolicy,
        AOSSPolicy: aossPolicy
      },
    });

    const managedFlinkRole = new iam.Role(this, 'Managed Flink Role', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      roleName: this.stackName+"-flink-role",
      description: 'Managed Flink BedRock Role',
      inlinePolicies: {
        BedRockRole: bedRockPolicy,
        KDAAccessPolicy: kdaAccessPolicy,
        AccessCWLogsPolicy: accessCWLogsPolicy,
        S3Policy: s3Policy,
        KinesisPolicy:kinesisPolicy,
        AOSSPolicy: aossPolicy
      },
    });

    // Access Policy
    const accessPolicy = [
      {
        "Rules": [
          {
            "Resource": [
              "collection/"+collectionName
            ],
            "Permission": [
              "aoss:CreateCollectionItems",
              "aoss:DeleteCollectionItems",
              "aoss:UpdateCollectionItems",
              "aoss:DescribeCollectionItems"
            ],
            "ResourceType": "collection"
          },
          {
            "Resource": [
              "index/"+collectionName+"/*"
            ],
            "Permission": [
              "aoss:CreateIndex",
              "aoss:DeleteIndex",
              "aoss:UpdateIndex",
              "aoss:DescribeIndex",
              "aoss:ReadDocument",
              "aoss:WriteDocument"
            ],
            "ResourceType": "index"
          }
        ],
        "Principal": [
            managedFlinkRole.roleArn,
          LambdaBedRockRole.roleArn
        ],
        "Description": "Rule 1"
      }
    ]


    // Check policy name length
    const accessPolicyName = `${collectionName}-access-policy`;

    // Create access policy
    const cfnAccessPolicy = new opensearchserverless.CfnAccessPolicy(this, 'OpssDataAccessPolicy', {
      name: accessPolicyName,
      description: 'Policy for data access',
      policy: JSON.stringify(accessPolicy),
      type: 'data',
    });

    // Example resource


    // Add dependencies
    cfnCollection.addDependency(cfnNetworkSecurityPolicy);
    cfnCollection.addDependency(cfnAccessPolicy);
    cfnCollection.addDependency(cfnEncryptionSecurityPolicy)






    const managedFlinkApplication = new kda.CfnApplication(this, 'Managed Flink Application', {
      applicationName: 'kds-msf-aoss',
      runtimeEnvironment: 'FLINK-1_18',
      serviceExecutionRole: managedFlinkRole.roleArn,
      applicationConfiguration: {
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: 'arn:aws:s3:::'+flinkApplicationJar.s3BucketName,
              fileKey: flinkApplicationJar.s3ObjectKey
            }
          },
          codeContentType: "ZIPFILE"
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: true
        },
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: 'FlinkApplicationProperties',
            propertyMap: {
              'os.domain': cfnCollection.attrCollectionEndpoint,
              'os.custom.index': 'kds-flink-aoss-rag-index',
              'kinesis.source.stream': kinesisStream.streamName,
              'region':this.region,
              'embedding.model': EmbeddingModel.valueAsString
            },
          }],
        },

        flinkApplicationConfiguration: {
          parallelismConfiguration: {
            parallelism: 1,
            configurationType: 'CUSTOM',
            parallelismPerKpu: 1,
            autoScalingEnabled: false

          },
          monitoringConfiguration: {
            configurationType: "CUSTOM",
            metricsLevel: "APPLICATION",
            logLevel: "INFO"
          },
        }

      }
    });


    const cfnApplicationCloudWatchLoggingOption= new kda.CfnApplicationCloudWatchLoggingOption(this,"managedFlinkLogs", {
      applicationName: "kds-msf-aoss",
      cloudWatchLoggingOption: {
        logStreamArn: "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName+":log-stream:" + logStream.logStreamName,
      },
    });

    managedFlinkApplication.node.addDependency(managedFlinkRole)


    const startFlinkApplicationHandler = new lambda.Function(this, "startFlinkApplicationHandler", {
      runtime: lambda.Runtime.PYTHON_3_12,
      code: lambda.Code.fromAsset("./startFlinkApplication"),
      handler: "index.on_event",
      timeout: cdk.Duration.minutes(14),
      memorySize: 512
    })

    const startFlinkApplicationProvider = new Provider(this, "startFlinkApplicationProvider", {
      onEventHandler: startFlinkApplicationHandler,
      logRetention: aws_logs.RetentionDays.ONE_WEEK
    })

    startFlinkApplicationHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "kinesisanalytics:DescribeApplication",
        "kinesisanalytics:StartApplication",
        "kinesisanalytics:StopApplication",

      ],
      resources: [`arn:aws:kinesisanalytics:${this.region}:${this.account}:application/`+managedFlinkApplication.applicationName]
    }))

    const startFlinkApplicationResource = new CustomResource(this, "startFlinkApplicationResource", {
      serviceToken: startFlinkApplicationProvider.serviceToken,
      properties: {
        AppName: managedFlinkApplication.applicationName,
      }
    })

    startFlinkApplicationResource.node.addDependency(managedFlinkApplication);
    startFlinkApplicationResource.node.addDependency(cfnCollection);

    cfnApplicationCloudWatchLoggingOption.node.addDependency(managedFlinkApplication)
    const kdsProducer = new lambda.Function(this, 'KDSProducer', {
      runtime: lambda.Runtime.PYTHON_3_10,
      functionName: "kds-producer",
      handler: 'lambda_function.lambda_handler',
      role: LambdaBedRockRole,
      code: lambda.Code.fromAsset('./kds-producer-lambda'),
      memorySize: 2048,
      timeout: cdk.Duration.seconds(300),
      environment: {
        kds: kinesisStream.streamName,
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["logs:CreateLogStream", "logs:PutLogEvents"],
          resources: ['arn:aws:logs:' + this.region + ":" + this.account + ':log-group:/aws/lambda/kds-producer'],
        })
      ]
    });
    const opensearchIndexCreationFunction = new lambda.Function(this, 'OpenSearch Index Creation Function', {
      runtime: lambda.Runtime.PYTHON_3_10,
      functionName: "opensearch-index-function",
      handler: 'lambda_function.lambda_handler',
      role: LambdaBedRockRole,
      code: lambda.Code.fromAsset('./index-creation-function'),
      memorySize: 2048,
      timeout: cdk.Duration.seconds(300),
      environment: {
        aosDomain: cfnCollection.attrCollectionEndpoint,
        embeddingModel: EmbeddingModel.valueAsString
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["logs:CreateLogStream", "logs:PutLogEvents"],
          resources: ['arn:aws:logs:' + this.region + ":" + this.account + ':log-group:/aws/lambda/opensearch-index-function'],
        })
      ]
    });

    // Add the layer using the layer ARN
    const boto3LayerARN = 'arn:aws:lambda:' + this.region + ':770693421928:layer:Klayers-p310-boto3:13';
    const opensearchLayerARN = 'arn:aws:lambda:' + this.region + ':770693421928:layer:Klayers-p38-opensearch-py:17';

    opensearchIndexCreationFunction.addLayers(lambda.LayerVersion.fromLayerVersionArn(this, 'Boto3Layer', boto3LayerARN), lambda.LayerVersion.fromLayerVersionArn(this, 'OpenSearchLayer', opensearchLayerARN));
    const LambdalogGroup = new logs.LogGroup(this, 'LambdaMyLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
      logGroupName: '/aws/lambda/'+opensearchIndexCreationFunction.functionName// Adjust retention as needed
    });

    const startLambdaIndexFunctionProvider = new Provider(this, "startLambdaIndexFunctionProvider", {
      onEventHandler: opensearchIndexCreationFunction,
      logRetention: aws_logs.RetentionDays.ONE_WEEK
    })

    const startLambdaIndexFunctionResource = new CustomResource(this, "startLambdaIndexFunctionResource", {
      serviceToken: startLambdaIndexFunctionProvider.serviceToken,
    })

    startLambdaIndexFunctionResource.node.addDependency(cfnCollection)



    // Outputs
    new cdk.CfnOutput(this, 'OpenSearchEndpoint', {
      value: cfnCollection.attrCollectionEndpoint,
      exportName: `${this.stackName}-OpenSearchEndpoint`,
    });



    new cdk.CfnOutput(this, 'DashboardsURL', {
      value: cfnCollection.attrDashboardEndpoint,
      exportName: `${this.stackName}-DashboardsURL`,
    });

  }


}
