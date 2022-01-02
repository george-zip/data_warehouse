## Project: Data Warehouse

### Overview

This project contains scripts to create and populate an AWS Redshift database as part of a data warehouse. The source of data is JSON files stored in S3. 

### Details

The project contains the following scripts:

[aws_dependencies_up.py](aws_dependencies_up.py) brings up the project's dependencies on AWS, including establishing IAM roles and the Redshift cluster.

[aws_dependencies_down.py](aws_dependencies_down.py) shuts down the Redshift cluster on AWS and deletes any IAM roles.

[create_tables.py](create_tables.py) runs queries to drop and create tables in Redshift [sql_queries.py](sql_queries.py).

[etl.py](etl.py) extracts raw data from S3 into staging tables then loads it into the final tables.

[sql_queries.py](sql_queries.py) contains SQL table creation, copy and insertion commands.

[run_data_quality_checks.py](run_data_quality_checks.py) runs verification checks on the populated database tables.

### How to run

To install the libraries needed to run this project, please run:

```commandline
pip install -r requirements.txt
```

The [dwh.cfg](dwh.cfg) file contains the AWS settings needed for each script to connect and run its queries. However, the AWS password and secret must be defined in your environment before running any script.

```commandline
export KEY=<key>
export SECRET=<secret>
```

```commandline
python aws_dependencies_up.py
python create_tables.py
python etl.py
python run_data_quality_checks.py
python aws_dependencies_down.py
```


