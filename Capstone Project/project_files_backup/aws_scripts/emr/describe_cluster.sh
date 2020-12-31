#!/bin/bash

CLUSTER_ID=$(aws emr list-clusters --active --output text --query 'Clusters[*].{ClusterId:Id}')

if [ -z "$CLUSTER_ID" ]
then
    echo "NO ACTIVE EMR CLUSTERS"
else
    aws emr describe-cluster \
        --cluster-id  ${CLUSTER_ID}\
        --output table \
        --query 'Cluster.{Name:Name,
                          Id:Id,
                          MasterPublicDnsName:MasterPublicDnsName,
                          State:Status.State}'
fi

