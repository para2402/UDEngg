#!/bin/bash

echo "Setting up your environment ..."

## For configuring AWS
mkdir ~/.aws
cd ~/.aws/

touch config
touch credentials

echo -e "[default]\nregion = us-west-2\noutput = table" > ~/.aws/config
echo -e "[default]\naws_access_key_id = AKIAY3KLFRX2UYH3OOMD\naws_secret_access_key = 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" > ~/.aws/credentials


## For loading custom AWS commands on startup
chmod 777 /home/workspace/aws_scripts/emr/list_clusters.sh
chmod 777 /home/workspace/aws_scripts/emr/describe_cluster.sh
chmod 777 /home/workspace/aws_scripts/emr/launch.sh
chmod 777 /home/workspace/aws_scripts/emr/terminate.sh

chmod 777 /home/workspace/aws_scripts/redshift/list_clusters.sh
chmod 777 /home/workspace/aws_scripts/redshift/launch.sh
chmod 777 /home/workspace/aws_scripts/redshift/terminate.sh

cp /home/workspace/utils/bashrc.backup ~/.bashrc
echo "Done."

exec bash