#!/bin/bash
#     --cluster-type multi-node \
#     --number-of-nodes 1 \

aws redshift create-cluster \
    --cluster-identifier olist-cluster \
    --cluster-type single-node \
    --node-type dc2.large \
    --db-name olistdb \
    --port 5439 \
    --master-username awsuser \
    --master-user-password Jaipara_2402 \
    --iam-roles arn:aws:iam::608432393717:role/dwhRole \
    --vpc-security-group-ids sg-07ccf075a9489c220 \
    --cluster-subnet-group-name udacity-cluster-subnet-group \
    --availability-zone us-west-2a \
    --publicly-accessible
