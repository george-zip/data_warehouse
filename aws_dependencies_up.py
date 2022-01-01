import configparser
import boto3
import os
import json
import time

"""
Create iam role and redshift cluster if they do not already exist. Otherwise, do nothing.
Update configuration file with IAM ARN and redshift endpoint so that values can be used in other
scripts.
"""


def cluster_exists(cluster_id: str, redshift_client: boto3.client) -> bool:
	# return True if redshift cluster `cluster_id` exists
	try:
		redshift_client.describe_clusters(
			ClusterIdentifier=cluster_id
		)
		return True
	except redshift_client.exceptions.ClusterNotFoundFault:
		pass
	return False


def role_exists(role_id: str, iam_client: boto3.client) -> bool:
	# return True if iam role `role_id` exists
	try:
		iam_client.get_role(RoleName=role_id)
		return True
	except iam_client.exceptions.NoSuchEntityException:
		pass
	return False


def update_configuration(config: configparser.ConfigParser, cluster_description: dict):
	# update configuration file with endpoint of server and IAM ARN
	print(f"DWH endpoint: {cluster_description['Endpoint']['Address']}")
	print(f"DWH role ARN: {cluster_description['IamRoles'][0]['IamRoleArn']}")
	config["CLUSTER"]["DWH_ENDPOINT"] = cluster_description['Endpoint']['Address']
	config["CLUSTER"]["DWH_ROLE_ARN"] = cluster_description['IamRoles'][0]['IamRoleArn']
	with open("dwh.cfg", "w") as file:
		config.write(file)


def create_iam_role(config: configparser.ConfigParser) -> boto3.client:
	# create iam role if it does not exist and attach s3 access policy
	# return constructed client
	iam = boto3.client(
		"iam",
		region_name=config.get("CLUSTER", "DWH_REGION"),
		aws_access_key_id=os.getenv("KEY"),
		aws_secret_access_key=os.getenv("SECRET")
	)
	if role_exists(config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"), iam):
		print(f"Role {config.get('IAM_ROLE', 'DWH_IAM_ROLE_NAME')} already exists")
	else:
		dwh_iam_role = iam.create_role(
			Path="/",
			RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"),
			Description="Allow Redshift to call AWS services",
			AssumeRolePolicyDocument=json.dumps(
				{"Statement": [{"Action": "sts:AssumeRole",
								"Effect": "Allow",
								"Principal": {"Service": "redshift.amazonaws.com"}}],
				 "Version": "2012-10-17"}
			)
		)
		iam.attach_role_policy(
			RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"),
			PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
		)
	return iam


def create_cluster(config: configparser.ConfigParser, iam: boto3.client) -> (boto3.client, dict):
	# create redshift cluster if it does not exist, wait until it is available
	# return client and cluster description
	redshift_client = boto3.client(
		"redshift",
		region_name=config.get("CLUSTER", "DWH_REGION"),
		aws_access_key_id=os.getenv("KEY"),
		aws_secret_access_key=os.getenv("SECRET")
	)
	if cluster_exists(
			config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
			redshift_client
	):
		print(f"Cluster {config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')} already exists")
	else:
		role_arn = iam.get_role(
			RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")
		)["Role"]["Arn"]
		try:
			redshift_client.create_cluster(
				ClusterType=config.get("CLUSTER", "DWH_CLUSTER_TYPE"),
				NodeType=config.get("CLUSTER", "DWH_NODE_TYPE"),
				NumberOfNodes=int(config.get("CLUSTER", "DWH_NUM_NODES")),
				DBName=config.get("CLUSTER", "DB_NAME"),
				ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
				MasterUsername=config.get("CLUSTER", "DB_USER"),
				MasterUserPassword=config.get("CLUSTER", "DB_PASSWORD"),
				IamRoles=[role_arn]
			)
		except Exception as e:
			print(f"Exception creating cluster: {e}")
		cluster_description = redshift_client.describe_clusters(
			ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
		)["Clusters"][0]
		while cluster_description["ClusterStatus"] == "creating":
			print(
				f"Waiting for redshift cluster to become available. Status: {cluster_description['ClusterStatus']}. "
				"Sleeping for 30 seconds."
			)
			time.sleep(30)
			cluster_description = redshift_client.describe_clusters(
				ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
			)["Clusters"][0]
	return redshift_client, cluster_description


def main():
	# run actions needed to create iam role, redshift cluster and vpc on aws
	config = configparser.ConfigParser()
	config.read('dwh.cfg')
	iam = create_iam_role(config)
	_, cluster_description = create_cluster(config, iam)
	update_configuration(config, cluster_description)
	print("Cluster is running")


if __name__ == "__main__":
	main()
