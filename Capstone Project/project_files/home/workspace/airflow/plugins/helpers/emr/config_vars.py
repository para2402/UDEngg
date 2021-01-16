JOB_FLOW_OVERRIDES = {
    "Name": "Olist Spark Cluster",
    "ReleaseLabel": "emr-5.32.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
#     "LogUri": "s3://jaipara-udacity-capstone/emr_logs/",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
#             {
#                 "Name": "Task",
#                 "Market": "ON_DEMAND",
#                 "InstanceRole": "TASK",
#                 "InstanceType": "m5.xlarge",
#                 "InstanceCount": 1,
#             },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
        "Ec2SubnetId": "subnet-03706948bcc3ca8df"
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "StepConcurrencyLevel": 10
}


##########################################################################################################


SPARK_STEPS = [
# TO ENABLE DEBUGGING IN EMR ENABLE THIS STEP FOR EACH SEQUENCE OF SPARK STEPS
#     {
#         'Name': 'Setup Hadoop Debugging',
#         'ActionOnFailure': 'TERMINATE_CLUSTER',
#         'HadoopJarStep': {
#             'Jar': 'command-runner.jar',
#             'Args': ['state-pusher-script']
#         },
#     },
    {
        "Name": "{{ params.STEP_NAME }}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--deploy-mode",
                "cluster",
                "{{ params.SPARK_SCRIPT }}",
                "{{ params.RAW_DATA_PATH }}",
                "{{ params.STAGING_DATA_PATH }}",
                "{{ params.TABLE }}"
            ],
        },
    },
]