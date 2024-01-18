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


sql = """INSERT INTO INS(col1,col2,col3,col4,col5,col6,col7,col8,metadata_id)
		 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING ins_id;"""

params = (sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9])


cur = conn.cursor()
cur.execute(sql, params)

ins_id = cur.fetchone()[0]

conn.commit()
conn.close

print(ins_id)

