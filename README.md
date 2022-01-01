## Project: Data Modeling with Postgres

### Background

This is a data warehousing project that is part of the Udacity Data Engineering Nanodegree. The goal is to design and populate an AWS Redshift schema that facilitates analytical queries for a music streaming application. The client has outgrown their current database and would like to move their data and ETL processes to the cloud. 

JSON log files hosted in S3 provide the source of raw song play data and other JSON files provide metadata about the music.

### Implementation

The project contains the following scripts:

[aws_dependencies_up.py](aws_dependencies_up.py) brings up the project's dependencies on AWS, including establishing IAM roles and the Redshift cluster. Waits until cluster is described as available. This may take several minutes.

[aws_dependencies_down.py](aws_dependencies_down.py) shuts down the Redshift cluster on AWS and deletes any IAM roles.

[create_tables.py](create_tables.py) drops and creates the schema, using queries in [sql_queries.py](sql_queries.py).

[etl.py](etl.py) extracts raw data from S3 into staging tables then loads it into the final tables.

[sql_queries.py](sql_queries.py) defines the SQL commands for schema creation and population.

[run_data_quality_checks.py](run_data_quality_checks.py) runs verification queries on the populated database tables.


### How to run

The versions used to build this application are in requirements.txt. To install these libraries, run

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


