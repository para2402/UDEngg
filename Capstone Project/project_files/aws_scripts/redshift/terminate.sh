#!/bin/bash

aws redshift delete-cluster \
    --cluster-identifier udacity-cluster \
    --skip-final-cluster-snapshot
