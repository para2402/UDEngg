#!/bin/bash

aws redshift describe-clusters \
    --output table \
    --query 'Clusters[*].{ClusterIdentifier:ClusterIdentifier,
                          ClusterStatus:ClusterStatus,
                          EndpointAddress:Endpoint.Address,
                          EndpointPort:Endpoint.Port,
                          IamRoleARN:IamRoles[0].IamRoleArn,
                          PubliclyAccessible:PubliclyAccessible,
                          NumberOfNodes:NumberOfNodes,
                          AvailabilityZone:AvailabilityZone}'
