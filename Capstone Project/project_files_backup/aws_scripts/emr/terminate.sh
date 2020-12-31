#!/bin/bash

if [ -z "$1" ]
then
    echo "Terminating AWS EMR clusters with clusterIDs: "
    aws emr list-clusters --active --output text --query 'Clusters[*].{ClusterId:Id}'
    aws emr terminate-clusters --cluster-ids `aws emr list-clusters --active --output text --query 'Clusters[*].{ClusterId:Id}'`
else
    echo "Terminating AWS EMR cluster with clusterId=[$1]"
    aws emr terminate-clusters --cluster-ids $1
fi

# aws emr terminate-clusters --cluster-ids $1
