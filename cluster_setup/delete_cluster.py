import boto3
import configparser
import os 

config = configparser.ConfigParser()
cfg_path = os.path.join(os.getcwd(),'dwh.cfg')
config.read(cfg_path)

# AWS
KEY=config.get("AWS", "KEY")
SECRET=config.get("AWS", "SECRET")
TOKEN=config.get("AWS", "TOKEN")

CLUSTER_ID=config.get("CLUSTER", "CLUSTER_ID")
DWH_IAM_ROLE=config.get("IAM_ROLE", "DWH_IAM_ROLE")

redshift=boto3.client(
    'redshift', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN)

iam=boto3.client(
    'iam', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN)


# delete - uncomment when needed
redshift.delete_cluster(ClusterIdentifier=CLUSTER_ID,  SkipFinalClusterSnapshot=True)

# remove IAM roles etc.
iam.detach_role_policy(RoleName=DWH_IAM_ROLE, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE)
