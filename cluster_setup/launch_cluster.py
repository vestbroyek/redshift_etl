import boto3
import configparser
import json 
import os 

config = configparser.ConfigParser()
cfg_path = os.path.join(os.getcwd(),'../dwh.cfg')
config.read(cfg_path)

# AWS
KEY=config.get("AWS", "KEY")
SECRET=config.get("AWS", "SECRET")
TOKEN=config.get("AWS", "TOKEN")
# IAM
DWH_IAM_ROLE=config.get("IAM_ROLE", "DWH_IAM_ROLE")
# REDSHIFT
CLUSTER_TYPE=config.get("CLUSTER", "CLUSTER_TYPE")
NODE_TYPE=config.get("CLUSTER", "NODE_TYPE")
NODES=int(config.get("CLUSTER", "NODES"))
CLUSTER_ID=config.get("CLUSTER", "CLUSTER_ID")

DB=config.get("REDSHIFT", "DB")
USER=config.get("REDSHIFT", "USER")
PWD=config.get("REDSHIFT", "PWD")
PORT=int(config.get("REDSHIFT", "PORT"))

"""
Steps:
-- Setup -- 
1. Create a role for Redshift 
2. Attach read access to S3 to this role
3. Fetch the role ARN

-- Launching the cluster -- 
1. Create the cluster
2. Get the endpoint (hostname)
3. Open a TCP port (in this case, allow from 0.0.0.0, which is insecure)
4. Authorise ingress
"""

# create an IAM role for Redshift
# create IAM client
iam=boto3.client(
    'iam', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN)

# create role for Redshift
try:
    dwhRole = iam.create_role(
    Path='/',
    RoleName=DWH_IAM_ROLE,
    AssumeRolePolicyDocument=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
            ]
        }               
    ))

except Exception as e:
    print(e)

# attach a policy to allow Redshift access to s3
iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)

# get the role ARN for the new role
roleArn=iam.get_role(
    RoleName=DWH_IAM_ROLE
)['Role']['Arn']

print(f'The role ARN is {roleArn}')

print('Creating cluster...')

# Launch a cluster
redshift=boto3.client(
    'redshift', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN)

response = redshift.create_cluster(        
    #HW
    ClusterType=CLUSTER_TYPE,
    NodeType=NODE_TYPE,
    NumberOfNodes=int(NODES),

    #Identifiers & Credentials
    DBName=DB,
    ClusterIdentifier=CLUSTER_ID,
    MasterUsername=USER,
    MasterUserPassword=PWD,
    
    #Roles (for s3 access)
    IamRoles=[roleArn]  
)