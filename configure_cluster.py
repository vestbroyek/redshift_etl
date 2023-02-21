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
DB=config.get("REDSHIFT", "DB")
USER=config.get("REDSHIFT", "USER")
PWD=config.get("REDSHIFT", "PWD")
PORT=int(config.get("REDSHIFT", "PORT"))

redshift=boto3.client(
    'redshift', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN)


# get cluster properties
PROPERTIES = redshift.describe_clusters(ClusterIdentifier=CLUSTER_ID)['Clusters'][0]
# note hostname (endpoint)
ENDPOINT = PROPERTIES['Endpoint']['Address']

print(f'The cluster endpoint is {ENDPOINT}')

# authorise ingress
ec2=boto3.resource(
    'ec2', 
    region_name='us-east-1', 
    aws_access_key_id=KEY, 
    aws_secret_access_key=SECRET,
    aws_session_token=TOKEN
    )
    
vpc = ec2.Vpc(id=PROPERTIES['VpcId'])

defaultSg = list(vpc.security_groups.all())[0]
try:
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(PORT),
        ToPort=int(PORT)
)

except Exception as e:
    print(e)