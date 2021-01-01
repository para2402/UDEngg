#!/bin/bash

aws emr list-clusters \
--active \
--output table \
--query 'Clusters[*].{ClusterId:Id,
                      ClusterName:Name,
                      State:Status.State,
                      StateChangeReason:Status.StateChangeReason}'