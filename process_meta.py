import psycopg2
import sys

hostname = 'localhost'
database = 'EDI834_SL'
username = 'postgres'
pwd = ''
port_id = 5431

conn = psycopg2.connect(
			host = hostname,
			dbname = database,
			user = username,
			password = pwd,
			port = port_id)


sql = """INSERT INTO metadata(filename,interchange_control_number,sponsorname,payername,processeddate)
			VALUES(%s,%s,%s,%s,%s) RETURNING metadata_id;"""
			
params = (sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])


cur = conn.cursor()
cur.execute(sql, params)

META_ID = cur.fetchone()[0]

conn.commit()
conn.close

print(META_ID)

