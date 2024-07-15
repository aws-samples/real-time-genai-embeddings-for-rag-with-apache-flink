import { CfnOutput, SecretValue, Stack, StackProps } from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { aws_msk as msk } from 'aws-cdk-lib';
import * as cr from 'aws-cdk-lib/custom-resources';

export interface MSKServerlessContructProps extends StackProps {
    account: string,
    region: string,
    vpc: ec2.Vpc,
    clusterName: string,
    mskSG: ec2.SecurityGroup,
}

export class MSKServerlessContruct extends Construct {
    public cfnMskServerlessCluster: msk.CfnServerlessCluster;
    public cfnClusterArnOutput: CfnOutput;
    public bootstrapServersOutput: CfnOutput;

    constructor(scope: Construct, id: string, props: MSKServerlessContructProps) {
        super(scope, id);

        // msk cluster
        this.cfnMskServerlessCluster = new msk.CfnServerlessCluster(this, 'MSKServerlessCluster', {
            clusterName: props.clusterName,

            // unauthenticated
            clientAuthentication: {
                sasl: {
                    iam: {
                        enabled: true,
                    },
                },
            },

            vpcConfigs: [{
                subnetIds: props.vpc.selectSubnets({
                    subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
                }).subnetIds,
                securityGroups: [props.mskSG.securityGroupId],
            }]

        }); // CfnCluster

        // 👇 create an output for cluster ARN
        this.cfnClusterArnOutput = new cdk.CfnOutput(this, 'ClusterArnServerlessOutput', {
            value: this.cfnMskServerlessCluster.attrArn,
            description: 'The ARN of our serverless MSK cluster',
            exportName: 'ServerlessMSKClusterARN-' + Date.now().toString(),
        });

        this.cfnClusterArnOutput.node.addDependency(this.cfnMskServerlessCluster);

        // custom resource policy to get bootstrap brokers for our cluster
        const getBootstrapBrokers = new cr.AwsCustomResource(this, 'BootstrapBrokersServerlessLookup', {
            onUpdate: {   // will also be called for a CREATE event
                service: 'Kafka',
                action: 'getBootstrapBrokers',
                parameters: {
                    ClusterArn: this.cfnMskServerlessCluster.attrArn
                },
                region: props.region,
                physicalResourceId: cr.PhysicalResourceId.of(Date.now().toString())
            },
            policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE })
        });

        getBootstrapBrokers.node.addDependency(this.cfnMskServerlessCluster);

        // 👇 create an output for bootstrap servers
        this.bootstrapServersOutput = new cdk.CfnOutput(this, 'ServerlessBootstrapServersOutput', {
            value: getBootstrapBrokers.getResponseField('BootstrapBrokerStringSaslIam'),
            description: 'List of bootstrap servers for our Serverless MSK cluster',
            exportName: 'ServerlessMSKBootstrapServers-' + Date.now().toString(),
        });

        this.bootstrapServersOutput.node.addDependency(getBootstrapBrokers);

    } // constructor
} // class MSKConstruct
