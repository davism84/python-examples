import mysql.connector as mysqldb

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

updatesql = "update ties_public.document_data set document_text = %s where document_id = %s"

conn=mysqldb.connect(host='localhost',user='',password='')

cur = conn.cursor()

cur.execute('select document_id, document_text from ties_public.document_data')

alldata = cur.fetchall()

i = 0
for rpttext in alldata:
	doc_id = rpttext[0]
	dirty = str(rpttext[1])
	cleaned = removeNonAscii(dirty)

	data = (cleaned, doc_id)

	updatecur = conn.cursor()
	updatecur.execute(updatesql, data)
	conn.commit()
	i = i + 1

conn.close()

print('Cleaned reports: ' + str(i))