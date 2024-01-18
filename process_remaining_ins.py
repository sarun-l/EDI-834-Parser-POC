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
			
code = sys.argv[1]

if code == 'REF':
	sql = """INSERT INTO "REF"(INS_id,col1,col2)
				VALUES(%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4])
	
elif code == 'NM1':
	sql = """INSERT INTO "NM1"(INS_id,col1,col2,col3,col4,col5,col6,col7,col8,col9)
				VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9],sys.argv[10],sys.argv[11])
	
elif code == 'N3':
	sql = """INSERT INTO "N3"(INS_id,col1)
				VALUES(%s,%s);"""
	params = (sys.argv[2],sys.argv[3])
				
elif code == 'N4':
	sql = """INSERT INTO "N4"(INS_id,col1,col2,col3)
				VALUES(%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
	
elif code == 'DMG':
	sql = """INSERT INTO "DMG"(INS_id,col1,col2,col3)
				VALUES(%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
	
elif code == 'HD':
	sql = """INSERT INTO "HD"(INS_id,col1,col2,col3,col4,col5)
				VALUES(%s,%s,%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7])
	
elif code == 'DTP':
	sql = """INSERT INTO "DTP"(INS_id,col1,col2,col3)
				VALUES(%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])

elif code == 'PER':
	sql = """INSERT INTO "PER"(INS_id,col1,col2,col3,col4)
				VALUES(%s,%s,%s,%s,%s);"""
	params = (sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6])
	
else:	
	print("NOT A RECGONIZED CODE")


cur = conn.cursor()
cur.execute(sql, params)

conn.commit()
conn.close

