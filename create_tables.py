import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
Drop and create redshift tables
SQL used is contained in sql_queries.py
"""


def drop_tables(cur, conn):
	# run table drop commands in drop_table_queries
	for query in drop_table_queries:
		cur.execute(query)
		conn.commit()


def create_tables(cur, conn):
	# run table creation commands in create_table_queries
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()




def main():
	# load configuration, connect to DB, drop then create tables
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

	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()


if __name__ == "__main__":
	main()
