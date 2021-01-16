#!/bin/bash

## DONT FORGET TO REMOVE AWS CREDENTIALS FROM THIS FILE


echo "Setting up your environment ..."
export PROJECT_WORKSPACE=/home/workspace
echo "PROJECT_WORKSPACE environment variable set to $PROJECT_WORKSPACE"

##########################################################################################

echo "Removing AWS cli 1.16.17 ..."
pip uninstall awscli -y

##########################################################################################

echo "Upgrading PyYAML, Markdown, Mako, Docutils ..."
pip install --quiet --ignore-installed PyYAML markdown mako docutils

##########################################################################################

echo "Installing Apache Airflow ..."
sudo apt-get update
sudo apt-get install build-essential -y

AIRFLOW_VERSION=2.0.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install --quiet "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

##########################################################################################

echo "Installing AWS CLI ..."
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip
sudo apt-get install -yy less

## For configuring AWS
echo "Configuring AWS CLI ..."
mkdir -p ~/.aws
cd ~/.aws/

touch config
touch credentials

echo -e "[default]\nregion = us-west-2\noutput = table" > ~/.aws/config
echo -e "[default]\naws_access_key_id = AKIAY3KLFRX2UYH3OOMD\naws_secret_access_key = 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" > ~/.aws/credentials

##########################################################################################

echo -e "\n\n##########################################################################" >> ~/.bashrc
echo "## ADDED BY JAIPARA" >> ~/.bashrc
echo "# Project Workspace" >> ~/.bashrc
echo "export PROJECT_WORKSPACE=/home/workspace" >> ~/.bashrc

echo "# AWS EMR" >> ~/.bashrc
echo "chmod --recursive 777 $PROJECT_WORKSPACE/utils/aws_scripts/" >> ~/.bashrc

echo "# alias emr-list='$PROJECT_WORKSPACE/utils/aws_scripts/emr/list_clusters.sh'" >> ~/.bashrc
echo "alias emr-list='$PROJECT_WORKSPACE/utils/aws_scripts/emr/describe_cluster.sh'" >> ~/.bashrc
echo "alias emr-describe='$PROJECT_WORKSPACE/utils/aws_scripts/emr/describe_cluster.sh'" >> ~/.bashrc
echo "alias emr-launch='$PROJECT_WORKSPACE/utils/aws_scripts/emr/launch.sh'" >> ~/.bashrc
echo "alias emr-terminate='$PROJECT_WORKSPACE/utils/aws_scripts/emr/terminate.sh'" >> ~/.bashrc

echo "# AWS Redshift" >> ~/.bashrc
echo "alias redshift-list='$PROJECT_WORKSPACE/utils/aws_scripts/redshift/list_clusters.sh'" >> ~/.bashrc
echo "alias redshift-launch='$PROJECT_WORKSPACE/utils/aws_scripts/redshift/launch.sh'" >> ~/.bashrc
echo "alias redshift-terminate='$PROJECT_WORKSPACE/utils/aws_scripts/redshift/terminate.sh'" >> ~/.bashrc

source ~/.bashrc

##########################################################################################

echo "Configuring PostgreSQL for Airflow ..."
sudo -u postgres psql -c "CREATE DATABASE airflow"
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow'"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow"

##########################################################################################

echo "Configuring Airflow ..."
# Apache Airflow Providers
pip install --quiet 'apache-airflow[amazon]'
pip install --quiet 'apache-airflow[apache.spark]'
pip install --quiet 'apache-airflow[postgres]'

airflow db init

python $PROJECT_WORKSPACE/configure_airflow.py

airflow scheduler --daemon
airflow webserver --daemon --port 3000
airflow users create --username admin --password admin \
                     --firstname jai --lastname v \
                     --role Admin --email fake.mail@mail.fake

##########################################################################################

sudo apt-get install -y nano

##########################################################################################

echo "Environment is ready!!"

exec bash
