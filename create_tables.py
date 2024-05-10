#!/usr/bin/env python
# coding: utf-8

# In[1]:


import configparser
import psycopg2
import sql_queries
from sql_queries import create_table_queries, drop_table_queries
import boto3
import json
import time


# In[2]:


def drop_tables(cur, conn, q):
    for query in q:
        print(query)
        cur.execute(query)
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("Done dropping tables")

def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("Done creating tables")

        
def create_clients_and_resources(KEY, SECRET):
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    print('Done creating ec2 resource')
    
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    print('Done creating s3 esource')
    
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                   region_name="us-west-2"
                    )
    print('Done creating iam client')
    
    redshift = boto3.client('redshift',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                        region_name="us-west-2"
                    )
    print('Done creating redshift client')
    
    return ec2, s3, iam, redshift
    

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
        )
        print('1.2 Attaching Policy')
        policy_arns = ["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"]
        for policy_arn in policy_arns: 
            iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=policy_arn)['ResponseMetadata']['HTTPStatusCode']
        # Get and print the IAM role ARN
        print('1.3 Get the IAM role ARN')
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(roleArn)

    except Exception as e:
        roleArn ='Not defined'
        print(e)
        
    return roleArn

def create_redshift_cluster(redshift, DWH_CLUSTER_TYPE,DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB,DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, IAM_ROLE_ARN):
    
    response = redshift.create_cluster(ClusterType=DWH_CLUSTER_TYPE, NodeType=DWH_NODE_TYPE, NumberOfNodes=int(DWH_NUM_NODES), DBName=DWH_DB, ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, MasterUsername=DWH_DB_USER, MasterUserPassword=DWH_DB_PASSWORD, IamRoles=[IAM_ROLE_ARN])
    print("Executed the code for redhsift create cluster")
    
        
def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    while True:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if cluster_props['ClusterStatus'] == "available":
            print(cluster_props['ClusterStatus'])
            print("Cluster status is available. We can now access the endpoint and other cluster props...")
            break
        elif cluster_props['ClusterStatus'] == "creating":
            print(cluster_props['ClusterStatus'])
            print("Cluster status not available yet. Waiting for status update...")
            time.sleep(120)  
        elif cluster_props['ClusterStatus'] == "deleted":
            print(cluster_props['ClusterStatus'])
            print("Cluster status is deleted. Don't worry about resource cost.")
            break
        elif cluster_props['ClusterStatus'] == "deleting":
            print(cluster_props['ClusterStatus'])
            print("Cluster status is deleted. Don't worry about resource cost.")
            time.sleep(120)
                 
    return cluster_props

def tcp_to_access_cluster(ec2,VpcId,DWH_PORT):
    vpc = ec2.Vpc(id=VpcId)
    defaultSg = list(vpc.security_groups.all())[0]
    print(list(vpc.security_groups.all()))
    print(defaultSg)
    print(defaultSg.group_name)
    defaultSg.authorize_ingress( GroupName=defaultSg.group_name, CidrIp='0.0.0.0/0', IpProtocol='TCP', FromPort=int(DWH_PORT),ToPort=int(DWH_PORT))
        
def delete_redshift_cluster(redshift,DWH_CLUSTER_IDENTIFIER):
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
def detach_resources(iam,DWH_IAM_ROLE_NAME):
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


# In[3]:


def main():
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    DWH_IAM_ROLE_NAME=config.get('CLUSTER', 'DWH_IAM_ROLE_NAME')
    KEY=config.get('AWS','KEY')
    SECRET=config.get('AWS', 'SECRET')
    
    ec2, s3, iam, redshift= create_clients_and_resources(KEY, SECRET)
    #roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    #print("Printing roleArn: ", roleArn)
    
    #Update IAM ARN with roleArn in config file
    # IAM_ROLE_ARN             = roleArn
    IAM_ROLE_ARN           = config.get("IAM_ROLE", "ARN")
    
    DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")
    
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER","DB_PORT")
    DWH_ENDPOINT           = config.get("CLUSTER","HOST")
    
    # create_redshift_cluster(redshift, DWH_CLUSTER_TYPE,DWH_NODE_TYPE, DWH_NUM_NODES,DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER,DWH_DB_PASSWORD, IAM_ROLE_ARN)
    # print ("Done executing code to create cluster")
    cluster_props=get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
    # print(cluster_props)
    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
    
    # print(DWH_ENDPOINT, DWH_ROLE_ARN)
    
    print(IAM_ROLE_ARN)
    
    
    # We need endpoint and dwh arn to issue copy command using ec2.
    
    # tcp_to_access_cluster(ec2,cluster_props['VpcId'],DWH_PORT)
    
    print(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT)
        
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    #print('Connected')
    
    q1 = "CREATE SCHEMA IF NOT EXISTS dist_sparkify;"
    q2 = "SET search_path TO dist_sparkify;"
    q = [q1, q2]
    
    drop_tables(cur, conn, q)
    create_tables(cur, conn)

    conn.close()
    
    # delete_redshift_cluster(redshift,DWH_CLUSTER_IDENTIFIER)
    
    
    # cluster_props=get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
    
    # detach_resources(iam,DWH_IAM_ROLE_NAME)
    
    # print('Deleted cluster, take a break!')


if __name__ == "__main__":
          main()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




