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
i2b2filename = "i2b2.sql"
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
conceptSqlStmts = []
i2b2SqlStmts = []
parentLevels = []

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
				path = TOP_PATH + PROJECT + '\\' + levels + '\\'+ pnode.strip() + '\\'
								
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
					isql = get_i2b2_insert_stmt(path.strip(), pnode, 'T', PROJECT, conceptId, 'FA')
					csql = get_concept_insert_stmt(path.strip(), pnode, PROJECT, conceptId)

					conceptSqlStmts.append(csql)
					i2b2SqlStmts.append(isql)

					#print(path)
					# now loop through all the codes
					for node in uniqCodes:
						try:
							if node.isnumeric() == False:
								conceptId = PROJECT[0:3] + ':' + str(random.random())
								conceptPath = path + str(node) + '\\'
								isql = get_i2b2_insert_stmt(conceptPath.strip(), str(node).strip(), 'T', PROJECT, conceptId, 'LA')
								csql = get_concept_insert_stmt(conceptPath, str(node), PROJECT, conceptId)
								conceptSqlStmts.append(csql)
								i2b2SqlStmts.append(isql)
						#print(sql)
						except:
							e = ''
					
				else:  # Numeric
					conceptId = PROJECT[0:3] + ':' + str(random.random())
					isql = get_i2b2_insert_stmt(path.strip(), str(pnode).strip(), 'N', PROJECT, conceptId, 'LA')
					csql = get_concept_insert_stmt(path.strip(), str(pnode).strip(), PROJECT, conceptId)
					conceptSqlStmts.append(csql)
					i2b2SqlStmts.append(isql)

		# add project root nodes
		conceptId = PROJECT[0:3] + ':' + str(random.random())
		isql = get_i2b2_insert_stmt(TOP_PATH + PROJECT + '\\', PROJECT, 'T', PROJECT, conceptId, 'FAS')
		csql = get_concept_insert_stmt(TOP_PATH + PROJECT + '\\', PROJECT, PROJECT, conceptId)	
		conceptSqlStmts.append(csql)
		i2b2SqlStmts.append(isql)						

		print('parent level nodes.............')
		# add parent level nodes
		for path in parentLevels:
			a = path.split('\\')
			pnode = a[len(a)-2]  # use the last label for the node name
			print(path)
			conceptId = PROJECT[0:3] + ':' + str(random.random())
			isql = get_i2b2_insert_stmt(path, pnode, 'T', PROJECT, conceptId, 'FA')
			csql = get_concept_insert_stmt(path, pnode, PROJECT, conceptId)	
			conceptSqlStmts.append(csql)
			i2b2SqlStmts.append(isql)

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass

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

def build_cf_levels():
	print('building cf levels...')
	for r in metadata:
		cat = r['Category Code']
		colno = r['Column Number']
		dl = r['Data Label']
		l = {'col':colno, 'label': dl, 'category':cat}
		cfgLevels.append(l)

def build_parent_level_nodes():
	print('building parent levels...')
	for r in metadata:
		cat = r['Category Code']
		path = cat.replace('+', '\\')

		# cycle through the 
		levels = path.split('\\')
		parentPath = TOP_PATH + PROJECT + '\\' 
		for p in levels:
			parentPath = parentPath + p + '\\'
			if parentPath not in parentLevels:
				parentLevels.append(parentPath)

def get_concept_insert_stmt(cPath, nodeName, projectId, conceptId):
	return 'insert into i2b2demodata.concept_dimension ' \
    		+ '(concept_cd,concept_path,name_char,update_date,download_date,import_date,sourcesystem_cd) ' \
    		+ 'values (\'' + conceptId + '\', \'' + cPath + '\', \'' + nodeName + '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + projectId + '\');'
    		#+ 'values (nextval(\'i2b2demodata.concept_id\'), \'' + cPath + '\', \'' + nodeName + '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + projectId + '\');'

def get_i2b2_insert_stmt(cPath, nodeName, dataType, projectId, conceptId, cVisual):
	levels = cPath.split('\\')
	try:
		hlevels = len(levels)-3   # account for begin and end \
	except:
		hlevels = 0

	#baseCode = '00000000'  # TODO: need to get the concept_code from concept dimension

	xml = ''

	# for numeric values use the XML structure
	if dataType != 'T':
		xml = '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'

	sql = 'insert into i2b2metadata.i2b2 (c_hlevel, c_fullname,c_name,c_visualattributes,c_synonym_cd,c_facttablecolumn,c_tablename,c_columnname,c_dimcode,c_tooltip,update_date,download_date' \
			+',import_date,sourcesystem_cd,c_basecode,c_operator,c_columndatatype,c_comment	,m_applied_path,c_metadataxml) values (' \
			+ str(hlevels) + ',\'' + cPath + '\', \'' + nodeName + '\', \'' + cVisual + '\',\'N\', \'CONCEPT_CD\', \'CONCEPT_DIMENSION\',\'CONCEPT_PATH\',' \
			+ '\'' + cPath + '\',' + '\'' + cPath + '\', current_timestamp, current_timestamp,current_timestamp,' + '\'' + projectId + '\', \'' + conceptId + '\',' \
			+ '\'LIKE\',\'' + dataType + '\', \'trial:'+ projectId + '\', \'@\',\'' + xml + '\');'

	return sql

# TODO this needs to figure out if path is a parent or a child node
def get_visual_node_level(hlevel):
	if hlevel > 2:
		return 'LA'
	return 'FA'

def main(cffile, dfile):
	read_cfg_file(cffile)
	build_cf_levels()
	build_parent_level_nodes()
	get_unique_codes(dfile)
	write_sql()


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("cfgfile", help="Config file")
	parser.add_argument("datafile", help="Data file")
	parser.add_argument("-p", help="Project name")
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
	# if args.o:
	# 	outfilename = args.o
	# if args.i:
	# 	fori2b2 = True

	main(cfgfile, datafile)