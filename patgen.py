# program to generate a PATIENT_DIMENSION SQL statements from a transmart configuration file 
import argparse
import csv
import requests
import json
import re
import string
import pandas as pd
import numpy as ny
import random

bulk = False
cfgfile = ''
datafile = ''
NAMESPACE = "cancer-reg"
KEYNS = "ties.model"
TAG_RE = re.compile(r'<[^>]+>')
transmart = False
delimiter = '\t'
outfilename = "patient_dim.sql"
bulkfilename = "patient_dim.csv"

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


def read_cfg_file(afile):
	print ('Reading CFG file...')	
	with open(afile) as csvfile:
		reader = csv.DictReader(csvfile, delimiter=delimiter)
		cols = reader.fieldnames

		for row in reader:
			#print(row)
			metadata.extend([{cols[i]:row[cols[i]] for i in range(len(cols))}])

	csvfile.close()

def build_patients(filename):
	try:
		print('Loading data file...' + filename)
		df = pd.read_csv(filename, delimiter='\t')  #dtype=str, 

		#headers = list(df)   # get a list of headers for the data
		#print (headers)
		#pats = df[headers[0]].unique()
		for i,row in df.iterrows():
			pid = row['SUBJ_ID']
			age = row['AGE']
			sex = row['SEX']
			race = row['RACE']
			srcId = PROJECT + ':'+ str(pid)

			if bulk:   #'patient_num': pid ,
				sql = {'sex_cd': sex[0:49], 'age_in_years_num': age, 'race_cd': race, 'update_date': 'now',    
					'download_date':'now', 'import_date': 'now', 'sourcesystem_cd':srcId}
			else:
				sql = get_insert_stmt(sex, age, race, srcId)

			sqlStmts.append(sql)
			#print(sql)

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


def get_insert_stmt(gender, age, race, srcId):
	return 'insert into i2b2demodata.patient_dimension ' \
			+ '(patient_num,sex_cd,age_in_years_num,race_cd,update_date,download_date,import_date,sourcesystem_cd)' \
    		+ 'values (nextval(\'i2b2demodata.seq_patient_num\'), \'' + gender + '\', ' + str(age) +  ', \'' + race + '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + srcId + '\');'


def write_bulk():

	#'patient_num',
	headers = ['sex_cd', 'age_in_years_num', 'race_cd', 'update_date', 
					'download_date', 'import_date', 'sourcesystem_cd']

	with open(bulkfilename, "w") as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter=',')
		writer.writeheader()
		for data in sqlStmts:
			writer.writerow(data)

def main(cffile, dfile):

	build_patients(dfile)

	if bulk:
		write_bulk()
	else:
		write_sql()


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("datafile", help="Data file")
	parser.add_argument("-p", help="Project name")
	parser.add_argument("-b", action="store_true", help="generate file for bulk load")	
#	parser.add_argument("-tab", action="store_true", help="tab delimited file (default comma)")
	args = parser.parse_args()

	if args.datafile:
		datafile = args.datafile
	if args.p:
		PROJECT = args.p
	if args.b:
		bulk = args.b

	main(cfgfile, datafile)