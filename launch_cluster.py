import boto3
import configparser
import json 
import os 

config = configparser.ConfigParser()
cfg_path = os.path.join(os.getcwd(),'dwh.cfg')
config.read(cfg_path)

# AWS
KEY=config.get("AWS", "KEY")
SECRET=config.get("AWS", "SECRET")
TOKEN=config.get("AWS", "TOKEN")
# IAM
DWH_IAM_ROLE=config.get("IAM_ROLE", "DWH_IAM_ROLE")
# REDSHIFT
CLUSTER_TYPE=config.get("REDSHIFT", "CLUSTER_TYPE")
NODE_TYPE=config.get("REDSHIFT", "NODE_TYPE")
NODES=int(config.get("REDSHIFT", "NODES"))
NODE_TYPE=config.get("REDSHIFT", "NODE_TYPE")
CLUSTER_ID=config.get("REDSHIFT", "CLUSTER_ID")
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

# attach a policy to allow Redshift access to s3
iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)

# get the role ARN for the new role
roleArn =iam.get_role(
    RoleName=DWH_IAM_ROLE
)

# Launch a cluster
redshift=boto3.client(
    'iam', 
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

# get cluster properties
PROPERTIES = redshift.describe_clusters(ClusterIdentifier=CLUSTER_ID)['Clusters'][0]
# note hostname (endpoint)
ENDPOINT = PROPERTIES['Endpoint']['Address']

# authorise ingress
ec2=boto3.resource(
    'ec2', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET.
    aws_session_token=TOKEN)
    
vpc = ec2.Vpc(id=PROPERTIES['VpcId'])

defaultSg = list(vpc.security_groups.all())[0]
defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp='0.0.0.0/0',
    IpProtocol='TCP',
    FromPort=int(PORT),
    ToPort=int(PORT)
)

"""
# delete - uncomment when needed
redshift.delete_cluster(ClusterIdentifier=CLUSTER_ID,  SkipFinalClusterSnapshot=True)

# remove IAM roles etc.
iam.detach_role_policy(RoleName=DWH_IAM_ROLE, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE)
"""