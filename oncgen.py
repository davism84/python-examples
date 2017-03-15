# program to generate a CFG for TIES from a set of CSV filesfor Cancer Registry
import argparse
import csv
import requests
import json
import re
import string
import pandas as pd
import numpy as ny
import random

cfgfile = ''
datafile = ''
NAMESPACE = "cancer-reg"
KEYNS = "ties.model"
TAG_RE = re.compile(r'<[^>]+>')
transmart = False
delimiter = '\t'
conceptfilename = "concepts_dim.sql"
bulkconceptfilename = "concepts_dim.csv"
i2b2filename = "i2b2.sql"
bulki2b2filename = "i2b2.csv"
TOP_PATH = '\\Public Studies\\'
PROJECT = ''
strMode = True
fori2b2 = False
diseaseCat = 'Breast'  # default

def remove_tags(text):
	tmp = TAG_RE.sub('', text)
	scrubbed = tmp.replace('&#8217;', '')
	return scrubbed

columns = ""
metadata = []
dataFrame = ''
cfgLevels = []
conceptSqlStmts = []
i2b2SqlStmts = []
parentLevels = []
conceptKeys = []

headers = {
	'x-seerapi-key': "bc70251be6e4dad034b029dcb199e6f5",'cache-control': "no-cache",
	'postman-token': "55e9c632-3587-11a4-0a5c-a6d32378761f"
	}

def read_cfg_file(afile):
	print ('Reading CFG file...')	
	with open(afile) as csvfile:
		reader = csv.DictReader(csvfile, delimiter=delimiter)
		cols = reader.fieldnames

		for row in reader:
			#print(row)
			metadata.extend([{cols[i]:row[cols[i]] for i in range(len(cols))}])

	csvfile.close()

def read_data_file(filename):
	try:
		print('Loading data file...' + filename)
		if strMode:
			return pd.read_csv(filename, dtype=str, delimiter='\t')
		else:
			return pd.read_csv(filename, delimiter='\t')
	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

def get_unique_codes(filename):
	print('getting unique codes...')
	try:
		print('Loading data file...' + filename)
		df = pd.read_csv(filename, delimiter='\t')  #dtype=str, 

		headers = list(df)   # get a list of headers for the data
		#print (df)
		sql =''
		#print(cfgLevels)

		for lvl in cfgLevels:
			colno = int(lvl['col'])
			if colno > 2:		# SKIP first columns assume SUBJ_ID and STUDY_ID
				cat = lvl['category']
				pnode = lvl['label']
				levels = cat.replace('+', '\\')
				tooltip = lvl['descrip']
				path = TOP_PATH + PROJECT + '\\' + diseaseCat + '\\' + levels + '\\'+ pnode.strip() + '\\'
								
				colType = df[headers[colno-1]].dtype
				#print(colType.name)
				
				# if its a string see if we have associated codes
				if colType.name == 'object':
					uniqCodes = df[headers[colno-1]].unique()  # get all unique codes
					#print(len(uniqCodes))
					
					# add parent node of the codes
					#a = levels.split('\\')
					#n = a[len(a)-1]  # use the last label for the node name
					conceptId = PROJECT[0:3] + ':' + str(random.random())
					get_i2b2_insert_stmt(path.strip(), pnode, 'T', PROJECT, conceptId, 'FA', colno, tooltip)
					get_concept_insert_stmt(path.strip(), pnode, PROJECT, conceptId)

					#print(path)
					# now loop through all the codes
					for node in uniqCodes:
						try:
							if node.isnumeric() == False:
								conceptId = PROJECT[0:3] + ':' + str(random.random())
								conceptPath = path + str(node) + '\\'
								get_i2b2_insert_stmt(conceptPath.strip(), str(node).strip(), 'T', PROJECT, conceptId, 'LA', colno, tooltip)
								get_concept_insert_stmt(conceptPath, str(node), PROJECT, conceptId)
						except:
							e = ''
					
				else:  # Numeric
					conceptId = PROJECT[0:3] + ':' + str(random.random())
					get_i2b2_insert_stmt(path.strip(), str(pnode).strip(), 'N', PROJECT, conceptId, 'LA', colno, tooltip)
					get_concept_insert_stmt(path.strip(), str(pnode).strip(), PROJECT, conceptId)

		# add project root nodes
		conceptId = PROJECT[0:3] + ':' + str(random.random())
		get_i2b2_insert_stmt(TOP_PATH + PROJECT + '\\', PROJECT, 'T', PROJECT, conceptId, 'FAS', colno, '')
		get_concept_insert_stmt(TOP_PATH + PROJECT + '\\', PROJECT, PROJECT, conceptId)	

		# add a disease layer
		if diseaseCat:
			conceptId = PROJECT[0:3] + ':' + str(random.random())
			get_i2b2_insert_stmt(TOP_PATH + PROJECT + '\\' + diseaseCat + '\\', diseaseCat, 'T', PROJECT, conceptId, 'FA', colno, diseaseCat)
			get_concept_insert_stmt(TOP_PATH + PROJECT + '\\' + diseaseCat + '\\', diseaseCat, PROJECT, conceptId)	

		print('parent level nodes.............')
		# add parent level nodes
		for path in parentLevels:
			a = path.split('\\')
			pnode = a[len(a)-2]  # use the last label for the node name
			#print(path)
			conceptId = PROJECT[0:3] + ':' + str(random.random())
			get_i2b2_insert_stmt(path, pnode, 'T', PROJECT, conceptId, 'FA', colno, pnode)
			get_concept_insert_stmt(path, pnode, PROJECT, conceptId)	

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

def build_cf_levels():
	print('building cf levels...')
	for r in metadata:
		cat = r['Category Code']
		colno = r['Column Number']
		dl = r['Data Label']
		tt = r['Description']
		if not tt:
			tt = dl

		l = {'col':colno, 'label': dl, 'category':cat, 'descrip': tt}
		cfgLevels.append(l)

def build_parent_level_nodes():
	print('building parent levels...')
	for r in metadata:
		cat = r['Category Code']
		path = cat.replace('+', '\\')

		# cycle through the 
		levels = path.split('\\')
		parentPath = TOP_PATH + PROJECT + '\\' + diseaseCat + '\\'
		for p in levels:
			parentPath = parentPath + p + '\\'
			if parentPath not in parentLevels:
				parentLevels.append(parentPath)

def get_concept_insert_stmt(cPath, nodeName, projectId, conceptId):
	if bulk:
		sql = {'concept_cd':conceptId,'concept_path':cPath, 'name_char': nodeName, 'update_date':'now', 'download_date':'now', 'import_date':'now','sourcesystem_cd':projectId}
	else:
		sql = 'insert into i2b2demodata.concept_dimension ' \
    		+ '(concept_cd,concept_path,name_char,update_date,download_date,import_date,sourcesystem_cd) ' \
    		+ 'values (\'' + conceptId + '\', \'' + cPath + '\', \'' + nodeName + '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + projectId + '\');'

	conceptSqlStmts.append(sql)

def get_i2b2_insert_stmt(cPath, nodeName, dataType, projectId, conceptId, cVisual, colno, tooltip):
	levels = cPath.split('\\')
	try:
		hlevels = len(levels)-3   # account for begin and end \
	except:
		hlevels = 0

	xml = ''

	# for numeric values use the XML structure
	if dataType != 'T':
		xml = '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'

	dataType = 'T'  # in i2b2 they are all put in as T for the c_columndatatype
	
	if bulk:
		sql = {'c_hlevel':hlevels, 'c_fullname':cPath,'c_name':nodeName,'c_visualattributes':cVisual,'c_synonym_cd':'N','c_facttablecolumn':'CONCEPT_CD',
				'c_tablename':'CONCEPT_DIMENSION','c_columnname':'CONCEPT_PATH','c_dimcode':cPath,'c_tooltip':tooltip,'update_date':'now','download_date':'now', 
				'import_date':'now','sourcesystem_cd':projectId,'c_basecode':conceptId,'c_operator':'LIKE','c_columndatatype':dataType,
				'c_comment':'trial:'+ projectId,'m_applied_path':'@','c_metadataxml':xml}
	else:
		sql = 'insert into i2b2metadata.i2b2 (c_hlevel, c_fullname,c_name,c_visualattributes,c_synonym_cd,c_facttablecolumn,c_tablename,c_columnname,c_dimcode,c_tooltip,update_date,download_date' \
			+',import_date,sourcesystem_cd,c_basecode,c_operator,c_columndatatype,c_comment	,m_applied_path,c_metadataxml) values (' \
			+ str(hlevels) + ',\'' + cPath + '\', \'' + nodeName + '\', \'' + cVisual + '\',\'N\', \'CONCEPT_CD\', \'CONCEPT_DIMENSION\',\'CONCEPT_PATH\',' \
			+ '\'' + cPath + '\',' + '\'' + tooltip + '\', now, now, now,' + '\'' + projectId + '\', \'' + conceptId + '\',' \
			+ '\'LIKE\',\'' + dataType + '\', \'trial:'+ projectId + '\', \'@\',\'' + xml + '\');'

	# save the sql
	i2b2SqlStmts.append(sql)

	#capture the concept key
	cKey = {'column': colno, 'node': nodeName, 'conceptId': conceptId}
	conceptKeys.append(cKey)

def write_sql():
	
	try:
		print('Writing concept dimension sql to ' + conceptfilename)
		outfile = open(conceptfilename, "w")

		for line in conceptSqlStmts:
			outfile.write(line)
			outfile.write('\n')

		print('Writing i2b2 sql to ' + i2b2filename)
		outfile2 = open(i2b2filename, "w")

		for line2 in i2b2SqlStmts:
			outfile2.write(line2)
			outfile2.write('\n')

	except:
		print ('Error writing line')
	finally:
		outfile.close()	
		outfile2.close()	

def write_concept_key_file():
	headers = ['column', 'node', 'conceptId']
	afile = conceptfilename + "-keys.tsv"

	with open(afile, "w") as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter='\t')
		writer.writeheader()
		for data in conceptKeys:
			writer.writerow(data)

def write_i2b2_bulk():

	headers = ['c_hlevel', 'c_fullname','c_name','c_visualattributes','c_synonym_cd','c_facttablecolumn','c_tablename','c_columnname','c_dimcode','c_tooltip','update_date','download_date', 
			'import_date','sourcesystem_cd','c_basecode','c_operator','c_columndatatype','c_comment','m_applied_path','c_metadataxml']

	with open(bulki2b2filename, encoding='utf-8', mode='w') as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter=',')
		writer.writeheader()
		for data in i2b2SqlStmts:
			writer.writerow(data)

def write_concepts_bulk():

	headers = ['concept_cd','concept_path', 'name_char', 'update_date', 'download_date', 'import_date','sourcesystem_cd']

	with open(bulkconceptfilename, encoding='utf-8', mode='w') as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter=',')
		writer.writeheader()
		for data in conceptSqlStmts:
			writer.writerow(data)


def main(cffile, dfile):
	read_cfg_file(cffile)
	build_cf_levels()
	build_parent_level_nodes()
	get_unique_codes(dfile)
	if bulk:
		write_i2b2_bulk()
		write_concepts_bulk()
	else:
		write_sql()

	write_concept_key_file()


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("cfgfile", help="Config file")
	parser.add_argument("datafile", help="Data file")
	parser.add_argument("-p", help="Project name")
	parser.add_argument("-d", help="Disease, will append to front of category, parent folder")
	parser.add_argument("-b", action="store_true", help="generate file for bulk load")		
#	parser.add_argument("-o", help="Output file")
#   parser.add_argument("-i", action="store_true", help="Generate sql for i2b2 table")	
#	parser.add_argument("-tab", action="store_true", help="tab delimited file (default comma)")
	args = parser.parse_args()

	if args.cfgfile:
		cfgfile = args.cfgfile
	if args.datafile:
		datafile = args.datafile
	if args.p:
		PROJECT = args.p
	if args.b:
		bulk = args.b	
	if args.d:
		diseaseCat = args.d	

	# if args.o:
	# 	outfilename = args.o
	# if args.i:
	# 	fori2b2 = True

	main(cfgfile, datafile)