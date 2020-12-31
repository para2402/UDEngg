#!/bin/bash

## This command is generated and copied from the AWS EMR Management Console 
## after launching the EMR cluster with the required configurations
## NOTE: If `--termination-protected` option is used, then the cluster cannot be terminated from the CLI

aws emr create-cluster \
--name 'udacity_spark_cluster' \
--release-label emr-6.2.0 \
--applications Name=Hadoop Name=Livy Name=Spark Name=JupyterEnterpriseGateway \
--ebs-root-volume-size 10 \
--ec2-attributes '{"KeyName":"udacity_spark_emr",
                   "InstanceProfile":"EMR_EC2_DefaultRole",
                   "SubnetId":"subnet-03706948bcc3ca8df",
                   "EmrManagedSlaveSecurityGroup":"sg-04a664a76ab6b8b4a",
                   "EmrManagedMasterSecurityGroup":"sg-04ae9045f174ef700"}' \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--service-role EMR_DefaultRole \
--instance-groups '[
                     {
                        "InstanceCount":1,
                        "EbsConfiguration":{
                           "EbsBlockDeviceConfigs":[
                              {
                                 "VolumeSpecification":{
                                    "SizeInGB":32,
                                    "VolumeType":"gp2"
                                 },
                                 "VolumesPerInstance":2
                              }
                           ]
                        },
                        "InstanceGroupType":"MASTER",
                        "InstanceType":"m5.xlarge",
                        "Name":"Master - 1"
                     },
                     {
                        "InstanceCount":1,
                        "BidPrice":"OnDemandPrice",
                        "EbsConfiguration":{
                           "EbsBlockDeviceConfigs":[
                              {
                                 "VolumeSpecification":{
                                    "SizeInGB":32,
                                    "VolumeType":"gp2"
                                 },
                                 "VolumesPerInstance":2
                              }
                           ]
                        },
                        "InstanceGroupType":"TASK",
                        "InstanceType":"m5.xlarge",
                        "Name":"Task - 3"
                     },
                     {
                        "InstanceCount":1,
                        "EbsConfiguration":{
                           "EbsBlockDeviceConfigs":[
                              {
                                 "VolumeSpecification":{
                                    "SizeInGB":32,
                                    "VolumeType":"gp2"
                                 },
                                 "VolumesPerInstance":2
                              }
                           ]
                        },
                        "InstanceGroupType":"CORE",
                        "InstanceType":"m5.xlarge",
                        "Name":"Core - 2"
                     }
                  ]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2 \
--enable-debugging \
--log-uri 's3n://jaipara-udacity-capstone/emr-logs/'
