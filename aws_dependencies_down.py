import configparser
import boto3
import os
import time
from aws_dependencies_up import cluster_exists, role_exists

"""
Shut down redshift cluster and delete IAM role
"""


def shut_down_cluster(config: configparser.ConfigParser):
	# shut down redshift cluster if it exists
	redshift_client = boto3.client(
		"redshift",
		region_name=config.get("CLUSTER", "DWH_REGION"),
		aws_access_key_id=os.getenv("KEY"),
		aws_secret_access_key=os.getenv("SECRET")
	)
	if not cluster_exists(
			config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
			redshift_client
	):
		print(f"Nothing to do for {config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')}")
	else:
		redshift_client.delete_cluster(
			ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
			SkipFinalClusterSnapshot=True
		)
		cluster_description = redshift_client.describe_clusters(
			ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
		)["Clusters"][0]
		print(cluster_description["ClusterStatus"])
		while cluster_description["ClusterStatus"] == "available":
			print(
				f"Waiting for redshift cluster to shut down. Status: {cluster_description['ClusterStatus']}. "
				"Sleeping for 30 seconds."
			)
			time.sleep(30)
			cluster_description = redshift_client.describe_clusters(
				ClusterIdentifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
			)["Clusters"][0]
		print("Cluster deleted")


def delete_role(config):
	# delete iam role if it exists
	iam = boto3.client(
		"iam",
		region_name=config.get("CLUSTER", "DWH_REGION"),
		aws_access_key_id=os.getenv("KEY"),
		aws_secret_access_key=os.getenv("SECRET")
	)
	if not role_exists(config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"), iam):
		print("No role to delete")
	else:
		iam.detach_role_policy(
			RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"),
			PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
		)
		iam.delete_role(RoleName=config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME"))
		print("iam role deleted")


def main():
	# shut down redshift client and delete role
	config = configparser.ConfigParser()
	config.read('dwh.cfg')
	shut_down_cluster(config)
	delete_role(config)


if __name__ == "__main__":
	main()
