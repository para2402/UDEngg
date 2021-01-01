#!/bin/bash

CLUSTER_IDS=$(aws emr list-clusters --active --output text --query 'Clusters[*].{ClusterId:Id}')

if [ -z "$CLUSTER_IDS" ]
then
    echo "NO ACTIVE EMR CLUSTERS"
else
    IFS=$'\n'
    read -rd '' -a CLUSTER_IDS_ARR <<< "$CLUSTER_IDS"
    echo -e "\nNumber of ACTIVE EMR Clusters: ${#CLUSTER_IDS_ARR[*]}"
    
    aws emr list-clusters \
        --active \
        --output table \
        --query 'Clusters[*].{ClusterId:Id,
                              ClusterName:Name,
                              State:Status.State}'
    
    echo -e "\nCluster Endpoints:"
    echo -e "------------------\n"

    # Print each value of the array by using the loop
    for id in "${CLUSTER_IDS_ARR[@]}";
    do
        aws emr describe-cluster \
            --cluster-id  ${id}\
            --output table \
            --query 'Cluster.[Id, Name, MasterPublicDnsName]'
    done
fi