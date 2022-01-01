import configparser
import psycopg2

data_quality_checks = [
	"select * from log_data_staging limit 5;",
	"select * from song_data_staging limit 5",
	"select * from songs limit 5",
	"select * from artists limit 5;",
	"select * from users limit 5;",
	"select * from time limit 5",
	"select sp.* from songplays sp, artists a where sp.artist_id = a.artist_id limit 5",
	"select sp.* from songplays sp, songs s where sp.song_id = s.song_id limit 5"
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
			raise RuntimeError(f"Query did not return results: {query}")
		print("-" * 10)

	# cur.execute("delete from log_data_staging where 1=1;")
	# cur.execute("delete from song_data_staging where 1=1;")
	# cur.execute("delete from songs where 1=1;")
	# cur.execute("delete from artists where 1=1;")
	# cur.execute("delete from users where 1=1;")
	# cur.execute("delete from time where 1=1;")
	# cur.execute("delete from songplays where 1=1;")
	# conn.commit()

	conn.close()


if __name__ == "__main__":
	main()
