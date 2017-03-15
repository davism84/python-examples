# program to generate a OBSERVATION_FACT SQL statements from a transmart configuration file 
# prequisites:  pip install pg 
import argparse
import csv
import requests
import json
import re
import string
import pandas as pd
import numpy as ny
import random
import psycopg2 as pg
import psycopg2.extras
import math
import sys

bulk = False
cfgfile = ''
datafile = ''
NAMESPACE = "cancer-reg"
KEYNS = "ties.model"
TAG_RE = re.compile(r'<[^>]+>')
transmart = False
delimiter = '\t'
outfilename = "observation_facts.sql"
bulkfilename = 'observation_facts.csv'
keyfilename = ""

TOP_PATH = '\\Public Studies\\'
PROJECT = ''
strMode = True
fori2b2 = False

def remove_tags(text):
	tmp = TAG_RE.sub('', text)
	scrubbed = tmp.replace('&#8217;', '')
	return scrubbed

columns = ""
metadata = []
dataFrame = ''
cfgLevels = []
sqlStmts = []
patientIds = []

dbcon = None
cur = None

def open_db():
	print('Opening database connection...')
	try:
		dbcon = pg.connect(host='localhost', database='transmart', user='i2b2demodata', password='i2b2demodata')
		cur = dbcon.cursor(cursor_factory=psycopg2.extras.DictCursor)
	except (Exception, pg.DatabaseError) as error:
		print(error)

# def get_patient_id(pid):
# 	#print('Getting patient id ', pid)
# 	try:
# 		iid = None
# 		sql = "select patient_num from patient_dimension where sourcesystem_cd = \'" + pid + "\'"
# 		dbcon = pg.connect(host='localhost', database='transmart', user='i2b2demodata', password='i2b2demodata')
# 		cur = dbcon.cursor()
		
# 		cur.execute(sql)
# 		rows = cur.fetchall()
# 		if rows:
# 			iid = rows[0][0]  # assume for row
# 			return iid
# 		return None

# 	except (Exception, pg.DatabaseError) as error:
# 		print(error)

def get_patient_id(pid):
	for v in patientIds:
		srcId = v['source_id']
		if pid == srcId:
			return v['patient_num']
	return None

def get_patient_ids_from_db():
	try:
		print('Loading patient ids for DB...')
		sql = "select patient_num, sourcesystem_cd from patient_dimension where sourcesystem_cd like \'" + PROJECT + "%\'"
		dbcon = pg.connect(host='localhost', database='transmart', user='i2b2demodata', password='i2b2demodata')
		cur = dbcon.cursor()
		
		cur.execute(sql)
		rows = cur.fetchall()
		if rows:
			for r in rows:
				patientIds.append({'patient_num': r[0], 'source_id': r[1]})

		print(len(patientIds))

		return len(patientIds)
	except (Exception, pg.DatabaseError) as error:
		print(error)
		return 0

def close_db():
	try:
		dbcon.close()	
		cur.close()
	except (Exception, pg.DatabaseError) as error:
		print(error)
	finally:
		if dbcon is not None:
			dbcon.close()

def process_status(cur, total):
	if cur == (total / 2):
		print ('Processed 50% {} of {}'.format(cur, total) )

def build_obs(filename):
	print('Loading data file...' + filename)
	try:
		df = pd.read_csv(filename, delimiter='\t')  #dtype=str, 

		headers = list(df)   # get a list of headers for the data
		srcId = PROJECT
		dfSize = len(df.index)
		
		print('     Total rows in data file:' + str(dfSize))
		print('Building OBS...')
		for i,row in df.iterrows():
			pid = row['SUBJ_ID']
			key = PROJECT + ':'+ str(pid)
			#print('.', end='')
			process_status(i, dfSize)

			patientNum = get_patient_id(key)
			#print(type(patientNum))
			#print(patientNum)
			# loop through all t he columns to get data
			if patientNum:
				for index in range(len(headers)):
					if headers[index] != 'SUBJ_ID' and headers[index] != 'STUDY_ID':   # skip these two fields
						tval = row[index]

						conceptId = None
						if tval:
							if type(tval) is str:
								valtype = 'T'
								tval = str(tval).strip()
								nval = ''
								if len(tval) > 0:	# make sure no empty strings get loaded
									conceptId = match(index+1, tval)
							elif math.isnan(tval) is False:
								valtype = 'N'
								nval = str(tval)
								tval = 'E'
								if len(nval) > 0:	# make sure no empty strings get loaded
									conceptId = match_num(index+1)

							# TODO:  need to lookup concept codes
							#conceptId = PROJECT[0:3] + ':' + str(random.random())
							if conceptId:
								if bulk:
									d = {'encounter_num': patientNum, 'patient_num': patientNum, 'concept_cd': conceptId, 'start_date': '', 
										'modifier_cd': '@', 'valtype_cd': valtype, 'tval_char': tval, 'nval_num': nval,
										'import_date': 'now', 'valueflag_cd': '@', 'provider_id': '@', 'location_cd': '@', 'instance_num': 1, 'sourcesystem_cd': srcId}
								else:
									d ='insert into i2b2demodata.observation_fact (encounter_num, patient_num, concept_cd, start_date, ' \
										'modifier_cd, valtype_cd, tval_char, nval_num, '\
										'import_date, valueflag_cd, provider_id, location_cd,instance_num, sourcesystem_cd)'\
    									+ 'values (' + str(patientNum) + ', ' + str(patientNum) + ', \'' + conceptId +  '\', ,' \
    									+ '\'@\',\'' + valtype + '\', \'' + tval + '\', ' + nval + ', now, \'@\', \'@\', \'@\', 1, ' + '\'' + srcId + '\');'

								sqlStmts.append(d)
								#print(d)

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

def write_sql():
	
	try:
		print('Writing sql to ' + outfilename)
		outfile = open(outfilename, "w")

		for line in sqlStmts:
			outfile.write(line)
			outfile.write('\n')

	except:		print ('Error writing line')
	finally:
		outfile.close()	

def write_bulk():

	headers = ['encounter_num', 'patient_num', 'concept_cd', 'start_date','modifier_cd', 'valtype_cd', 'tval_char', 
		'nval_num','import_date', 'valueflag_cd', 'provider_id', 'location_cd', 'instance_num', 'sourcesystem_cd']

	with open(bulkfilename, encoding='utf-8', mode='w') as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter=',')
		writer.writeheader()
		for data in sqlStmts:
			writer.writerow(data)

def get_insert_stmt(pid, concept, data, srcId):
	return 'insert into i2b2demodata.observation_fact (encounter_num, patient_num, concept_cd, start_date, ' \
			'modifier_cd, valtype_cd, tval_char, nval_num, units_cd, sourcesystem_cd, '\
			'import_date, valueflag_cd, provider_id, location_cd,instance_num)'\
    		+ 'values (\'' + pid + '\'), \'' + pid + '\', \'' + concept +  ', \'' + data \
    		+ '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + srcId + '\');'

def read_key_file():
	print ('Reading concept key file...')	
	with open(keyfilename) as csvfile:
		reader = csv.DictReader(csvfile, delimiter=delimiter)
		cols = reader.fieldnames

		for row in reader:
			#print(row)
			metadata.extend([{cols[i]:row[cols[i]] for i in range(len(cols))}])

	csvfile.close()

# match against metadata, returning phi, naa code
def match(col, val):
	for r in metadata:
		#print(type(r))
		column = r['column']
		node = r['node']
		cpid = r['conceptId']
		if  col == int(column) and val == node:
			return cpid
	return None

# match for numbers which is just the column
def match_num(col):
	for r in metadata:
		#print(type(r))
		column = r['column']
		cpid = r['conceptId']
		if  col == int(column):
			return cpid
	return None


def main(cffile, dfile):

	#open_db()
	patsLoaded = get_patient_ids_from_db()

	if patsLoaded < 1:
		print ("No patients have been loaded into the database.  Please load these first")
	else:
		read_key_file()

		build_obs(dfile)

		if bulk:
			write_bulk()
		else:
			write_sql()

	#close_db()

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("datafile", help="Data file")
	parser.add_argument("-p", help="Project name")
	parser.add_argument("-o", help="Output name")
	parser.add_argument("-k", help="concept key name")
	parser.add_argument("-b", action="store_true", help="generate file for bulk load")
#	parser.add_argument("-tab", action="store_true", help="tab delimited file (default comma)")
	args = parser.parse_args()

	if args.datafile:
		datafile = args.datafile
	if args.p:
		PROJECT = args.p
	if args.o:
		outfilename = args.o
	if args.k:
		keyfilename = args.k		
	if args.b:
		bulk = args.b

	#print (sys.argv[1:])

	main(cfgfile, datafile)