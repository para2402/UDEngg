#!/bin/bash

aws redshift create-cluster \
    --cluster-identifier udacity-cluster \
    --cluster-type multi-node \
    --node-type dc2.large \
    --number-of-nodes 2 \
    --db-name udacitydb \
    --port 5439 \
    --master-username awsuser \
    --master-user-password Omsairam_2402 \
    --iam-roles arn:aws:iam::608432393717:role/dwhRole \
    --vpc-security-group-ids sg-07ccf075a9489c220 \
    --cluster-subnet-group-name udacity-cluster-subnet-group \
    --availability-zone us-west-2a \
    --publicly-accessible
