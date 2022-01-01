import configparser
import psycopg2

data_quality_checks = [
	"select * from log_data_staging limit 5;",
	"select * from song_data_staging limit 5",
	# "select * from stl_load_errors;"
]


def main():
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

	for query in data_quality_checks:
		cur.execute(query)
		results = cur.fetchall()
		if len(results):
			for r in results:
				print(r)
		else:
			raise RuntimeError(f"Query did not return results {query}")

	conn.close()


if __name__ == "__main__":
	main()
