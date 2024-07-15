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
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import {MSKServerlessContruct} from "./msk-serverless-construct";
import * as python from '@aws-cdk/aws-lambda-python-alpha'
import {SubnetType} from "aws-cdk-lib/aws-ec2";

export class MsksFlinkBedrockOpensearchServerless extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const EmbeddingModel = new CfnParameter(this, "EmbeddingModel", {
      type: "String",
      description: "Which Amazon Bedrock Model use for Embeddings (Titan V1 , Titan V2"});


    //Jar Connectors
    const flinkApplicationJar = new assets.Asset(this, 'flinkApplicationJar', {
      path: ('./flink-bedrock/target/flink-bedrock-1.0-SNAPSHOT.jar'),
    });

    const vpc = new ec2.Vpc(this, 'VPC', {
      enableDnsHostnames: true,
      enableDnsSupport: true,
      maxAzs: 3,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'public-subnet',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'private-subnet',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        }
      ],
    });

    // security group for MSK access
    const mskSG = new ec2.SecurityGroup(this, 'mskSG', {
      vpc: vpc,
      allowAllOutbound: true,
      description: 'MSK Security Group'
    });

    mskSG.connections.allowInternally(ec2.Port.allTraffic(), 'Allow all traffic between hosts having the same security group');

    // instantiate serverless MSK cluster w/ IAM auth
    const serverlessMskCluster = new MSKServerlessContruct(this, 'MSKServerless', {
      account: this.account,
      region: this.region,
      vpc: vpc,
      clusterName: this.stackName,
      mskSG: mskSG,
    });

    serverlessMskCluster.node.addDependency(vpc);

    //AccessVPCPolicy
    const accessVPCPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['ec2:DeleteNetworkInterface',
            'ec2:DescribeDhcpOptions',
            'ec2:DescribeSecurityGroups',
            'ec2:CreateNetworkInterface',
            'ec2:DescribeNetworkInterfaces',
            'ec2:CreateNetworkInterfacePermission',
            'ec2:DescribeVpcs',
            'ec2:DescribeSubnets'],
        }),
      ],
    });

    //Access MSK
    const accessMSKPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["*"],
          actions: ['kafka-cluster:*'],
        }),
      ],
    });



    const collectionName = 'msks-vector-db'; // Example collection name
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
        MSKPolicy: accessMSKPolicy,
        AOSSPolicy: aossPolicy,
        VPCPolicy: accessVPCPolicy
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
        MSKPolicy: accessMSKPolicy,
        AOSSPolicy: aossPolicy,
        VPCPolicy: accessVPCPolicy
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
      applicationName: 'msk-msf-aoss',
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
        vpcConfigurations: [ {
          subnetIds: vpc.selectSubnets({
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          }).subnetIds,
          securityGroupIds: [mskSG.securityGroupId]
        }
        ],
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: 'FlinkApplicationProperties',
            propertyMap: {
              'os.domain': cfnCollection.attrCollectionEndpoint,
              'os.custom.index': 'msk-flink-aoss-rag-index',
              'source.bootstrap.servers': serverlessMskCluster.bootstrapServersOutput.value,
              'source.topic': 'msk-rag-topic',
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
      applicationName: "msk-msf-aoss",
      cloudWatchLoggingOption: {
        logStreamArn: "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+logGroup.logGroupName+":log-stream:" + logStream.logStreamName,
      },
    });

    managedFlinkApplication.node.addDependency(managedFlinkRole)
    managedFlinkApplication.node.addDependency(serverlessMskCluster)

    const kafkaClientLayer = new python.PythonLayerVersion(this, 'MyLayer', {
      entry: './lambda-python-layer/',
      compatibleArchitectures: [lambda.Architecture.X86_64],
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
      bundling: {
        platform: 'linux/amd64',
      }
    })

    const mskproducer = new lambda.Function(this, "mskproducer", {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'lambda_function.lambda_handler',
      role: LambdaBedRockRole,
      code: lambda.Code.fromAsset('./msk-producer-lambda'),
      vpc:vpc,
      securityGroups:[mskSG],
      vpcSubnets: {subnetType: SubnetType.PRIVATE_WITH_EGRESS},
      timeout: cdk.Duration.minutes(14),
      memorySize: 512,
      environment: {
        BOOTSTRAP_SERVERS: serverlessMskCluster.bootstrapServersOutput.value,
        region: this.region,
        TOPIC_NAME: 'msk-rag-topic',
      },
    })

    mskproducer.addLayers(kafkaClientLayer);

    const createTopic = new lambda.Function(this, "createTopic", {
      runtime: lambda.Runtime.PYTHON_3_11,
      code: lambda.Code.fromAsset("./topicCreation"),
      vpc:vpc,
      securityGroups:[mskSG],
      vpcSubnets: {subnetType: SubnetType.PRIVATE_WITH_EGRESS},
      role: LambdaBedRockRole,
      handler: "index.on_event",
      timeout: cdk.Duration.minutes(14),
      memorySize: 512
    })

    createTopic.addLayers(kafkaClientLayer)

    const createTopicProvider = new Provider(this, "createTopicProvider", {
      onEventHandler: createTopic,
      logRetention: aws_logs.RetentionDays.ONE_WEEK
    })

    createTopic.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "kafka-cluster:*"

      ],
      resources: [serverlessMskCluster.cfnClusterArnOutput.value]
    }))

    const createTopicResource = new CustomResource(this, "createTopicResource", {
      serviceToken: createTopicProvider.serviceToken,
      properties: {
        topic_name: 'msk-rag-topic',
        region: this.region,
        bootstrap_servers: serverlessMskCluster.bootstrapServersOutput.value,
        num_partitions: 3,
        replication_factor:3
      }
    })



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
    startFlinkApplicationResource.node.addDependency(createTopicResource);

    cfnApplicationCloudWatchLoggingOption.node.addDependency(managedFlinkApplication)

    const opensearchIndexCreationFunction = new lambda.Function(this, 'OpenSearch Index Creation Function', {
      runtime: lambda.Runtime.PYTHON_3_10,
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
