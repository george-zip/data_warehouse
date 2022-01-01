import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
Skeleton code to extract data from S3 into staging and then copy to final tables
Copy and insert commands are in sql_queries.py
"""


def load_staging_tables(cur, conn):
    # extract from S3 into staging tables
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # insert into final from staging tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # load configuration, connect to DB, then run extract and load process
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        f"host={config.get('CLUSTER', 'DWH_ENDPOINT')} "
        f"dbname={config.get('CLUSTER', 'DB_NAME')} "
        f"user={config.get('CLUSTER', 'DB_USER')} "
        f"password={config.get('CLUSTER', 'DB_PASSWORD')} "
        f"port={config.get('CLUSTER', 'DB_PORT')}"
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Staging tables loaded")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()