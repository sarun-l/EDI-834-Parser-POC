import psycopg2


hostname = 'localhost'
database = 'edi834'
username = 'postgres'
pwd = '12345'
port_id = 5431

conn = psycopg2.connect(
			host = hostname,
			dbname = database,
			user = username,
			password = pwd,
			port = port_id)


def process_metadata(filename,interchange_control_number,sponsorname,payername,processeddate):
	sql = """INSERT INTO metadata(filename,interchange_control_number,sponsorname,payername,processeddate)
				VALUES(%s,%s,%s,%s,%s) RETURNING metadata_id;"""
				
	params = (filename,interchange_control_number,sponsorname,payername,processeddate)

	cur = conn.cursor()
	cur.execute(sql, params)

	META_ID = cur.fetchone()[0]

	conn.commit()
	return META_ID
	conn.close()


def process_ins_line(col1,col2,col3,col4,col5,col6,col7,col8,metadata_id):
	sql = """INSERT INTO INS(col1,col2,col3,col4,col5,col6,col7,col8,metadata_id)
		 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING ins_id;"""

	params = (col1,col2,col3,col4,col5,col6,col7,col8,metadata_id)
	
	cur = conn.cursor()
	cur.execute(sql, params)

	ins_id = cur.fetchone()[0]

	conn.commit()
	return ins_id
	conn.close()

	print(ins_id)    
        
	
def process_remaining_ins(*args):
	code = args[0]
	
	if code == 'REF':
		sql = """INSERT INTO "REF"(INS_id,col1,col2)
					VALUES(%s,%s,%s);"""
		params = (args[1],args[2],args[3])
		
	elif code == 'NM1':
		sql = """INSERT INTO "NM1"(INS_id,col1,col2,col3,col4,col5,col6,col7,col8,col9)
					VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8],args[9],args[10])
		
	elif code == 'N3':
		sql = """INSERT INTO "N3"(INS_id,col1)
					VALUES(%s,%s);"""
		params = (args[1],args[2])
					
	elif code == 'N4':
		sql = """INSERT INTO "N4"(INS_id,col1,col2,col3)
					VALUES(%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4])
		
	elif code == 'DMG':
		sql = """INSERT INTO "DMG"(INS_id,col1,col2,col3)
					VALUES(%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4])
		
	elif code == 'HD':
		sql = """INSERT INTO "HD"(INS_id,col1,col2,col3,col4,col5)
					VALUES(%s,%s,%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4],args[5],args[6])
		
	elif code == 'DTP':
		sql = """INSERT INTO "DTP"(INS_id,col1,col2,col3)
					VALUES(%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4])

	elif code == 'PER':
		sql = """INSERT INTO "PER"(INS_id,col1,col2,col3,col4)
					VALUES(%s,%s,%s,%s,%s);"""
		params = (args[1],args[2],args[3],args[4],args[5])
		
	else:	
		print("NOT A RECOGNIZED CODE")


	cur = conn.cursor()
	cur.execute(sql, params)

	conn.commit()
	conn.close()


