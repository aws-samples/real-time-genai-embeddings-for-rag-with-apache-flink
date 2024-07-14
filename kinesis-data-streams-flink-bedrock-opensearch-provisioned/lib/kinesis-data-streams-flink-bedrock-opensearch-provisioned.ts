import * as cdk from 'aws-cdk-lib';
import {aws_logs, CfnParameter, CustomResource, RemovalPolicy, SecretValue} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2'
import * as assets from 'aws-cdk-lib/aws-s3-assets'
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {Provider} from "aws-cdk-lib/custom-resources";
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import {EngineVersion} from "aws-cdk-lib/aws-opensearchservice";
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import {AnyPrincipal} from "aws-cdk-lib/aws-iam";
import {IpAddresses} from "aws-cdk-lib/aws-ec2";


export class KinesisDataStreamsFlinkBedrockOpensearchProvisioned extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const EmbeddingModel = new CfnParameter(this, "EmbeddingModel", {
      type: "String",
      description: "Which Amazon Bedrock Model use for Embeddings (Titan V1 , Titan V2"});

    const IPAddress = new CfnParameter(this, "IPAddress", {
      type: "String",
      description: "IP Address to access OpenSearch Dashboard. It has to be in CIDR Range format"});



    //Jar Connectors
    const flinkApplicationJar = new assets.Asset(this, 'flinkApplicationJar', {
      path: ('./flink-bedrock/target/flink-bedrock-1.0-SNAPSHOT.jar'),
    });
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
      streamName: "kds-aos-source-rag",
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

    const aosPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['es:*']
        }),
      ],
    });

    const opensearchRAGDatabase = new opensearch.Domain(this, 'opensearchVectorDB', {
      version: EngineVersion.OPENSEARCH_2_11,
      removalPolicy: RemovalPolicy.DESTROY,
      nodeToNodeEncryption: true,
      enforceHttps: true,
      capacity: {
        dataNodes: 1,
        masterNodes: 0,
        multiAzWithStandbyEnabled: false,
        dataNodeInstanceType: "m6g.large.search"
      },
      ebs: {
        volumeSize: 100,
        volumeType: ec2.EbsDeviceVolumeType.GENERAL_PURPOSE_SSD_GP3
      }
    })



    const LambdaBedRockRole = new iam.Role(this, 'Lambda BedRock Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: this.stackName+"bedrock-role",
      description: 'BedRock Role',
      inlinePolicies: {
        BedRockRole: bedRockPolicy,
        KinesisPolicy: kinesisPolicy,
        AOSSPolicy: aosPolicy
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
        AOSSPolicy: aosPolicy
      },
    });

    opensearchRAGDatabase.addAccessPolicies(
        new iam.PolicyStatement({
          principals: [new iam.ArnPrincipal(managedFlinkRole.roleArn),new iam.ArnPrincipal(LambdaBedRockRole.roleArn)],
          actions: ['es:ESHttp*'],
          resources: [opensearchRAGDatabase.domainArn + "/*"],
        }),
        new iam.PolicyStatement({
          principals:[new AnyPrincipal()],
          actions: ['es:ESHttp*'],
          resources: [opensearchRAGDatabase.domainArn + "/*"],
          conditions: {
            'IpAddress': {
              'aws:SourceIp': IPAddress.valueAsString
            }
          }

        })
    );

    opensearchRAGDatabase.grantReadWrite(managedFlinkRole)
    opensearchRAGDatabase.grantReadWrite(LambdaBedRockRole)

    const managedFlinkApplication = new kda.CfnApplication(this, 'Managed Flink Application', {
      applicationName: 'kds-msf-aos',
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
              'os.domain': opensearchRAGDatabase.domainEndpoint,
              'os.custom.index': 'kds-flink-aos-rag-index',
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
      applicationName: "kds-msf-aos",
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

    cfnApplicationCloudWatchLoggingOption.node.addDependency(managedFlinkApplication)
    const kdsProducer = new lambda.Function(this, 'KDSProducer', {
      runtime: lambda.Runtime.PYTHON_3_10,
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
          resources: ['arn:aws:logs:' + this.region + ":" + this.account + ':log-group:/aws/lambda/*'],
        })
      ]
    });
    const opensearchIndexCreationFunction = new lambda.Function(this, 'OpenSearch Index Creation Function', {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'lambda_function.lambda_handler',
      role: LambdaBedRockRole,
      code: lambda.Code.fromAsset('./index-creation-function'),
      memorySize: 2048,
      timeout: cdk.Duration.seconds(300),
      environment: {
        aosDomain: opensearchRAGDatabase.domainEndpoint,
        embeddingModel: EmbeddingModel.valueAsString
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["logs:CreateLogStream", "logs:PutLogEvents"],
          resources: ['arn:aws:logs:' + this.region + ":" + this.account + ':log-group:/aws/lambda/*'],
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

    startLambdaIndexFunctionResource.node.addDependency(opensearchRAGDatabase)


    // Outputs
    new cdk.CfnOutput(this, 'OpenSearchEndpoint', {
      value: opensearchRAGDatabase.domainEndpoint,
      exportName: `${this.stackName}-OpenSearchEndpoint`,
    });


  }


}
