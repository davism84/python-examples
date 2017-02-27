# program to generate a CFG for TIES from a set of CSV filesfor Cancer Registry
import argparse
import csv
import requests
import json
import re
import string
import pandas as pd
import numpy as ny

cfgfile = ''
datafile = ''
NAMESPACE = "cancer-reg"
KEYNS = "ties.model"
TAG_RE = re.compile(r'<[^>]+>')
transmart = False
delimiter = '\t'
outfilename = "config.cfg"
TOP_PATH = '\\Public Studies\\'
PROJECT = ''
strMode = True

def remove_tags(text):
	tmp = TAG_RE.sub('', text)
	scrubbed = tmp.replace('&#8217;', '')
	return scrubbed

columns = ""
metadata = []
dataFrame = ''
cfgLevels = []

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

		for lvl in cfgLevels:
			colno = int(lvl['col'])
			if colno > 2:		# SKIP first columns assume SUBJ_ID and STUDY_ID
				cat = lvl['category']
				pnode = lvl['label']
				levels = cat.replace('+', '\\')
				path = TOP_PATH + levels + '\\'+ pnode.strip() + '\\'
				print(path)
				colType = df[headers[colno-1]].dtype
				print(type(colType))

				if colType == ny.dtype('object'):
					uniqCodes = df[headers[colno-1]].unique()
					for node in uniqCodes:
						try:
							if node.isnumeric() == False:
								conceptPath = path + str(node)
								#sql = get_insert_stmt(conceptPath, str(node), PROJECT)
								sql = get_i2b2_insert_stmt(conceptPath.strip(), str(node).strip(), 'T', PROJECT)
								#print(sql)
						except:
							e = ''
				else:
					sql = get_i2b2_insert_stmt(path.strip(), str(pnode).strip(), 'N', PROJECT)
					#print(sql)
						

	except Exception as e:
		raise
	else:
		pass
	finally:
		pass


# def read_data_file(afile):
# 	with open(afile) as csvfile:
# 		reader = csv.DictReader(csvfile, delimiter=delimiter)
# 		cols = reader.fieldnames

# 		for row in reader:
# 			#print(row)
# 			data.extend([{cols[i]:row[cols[i]] for i in range(len(cols))}])

# 	csvfile.close()

def build_cf_levels():
	print('building cf levels...')
	for r in metadata:
		cat = r['Category Code']
		colno = r['Column Number']
		dl = r['Data Label']
		l = {'col':colno, 'label': dl, 'category':cat}
		cfgLevels.append(l)
	#print(l)

# def parse_category():
# 	cfgLevels = []
# 	for r in metadata:
# 		cat = r['Category Code']
# 		colno = r['Column Number']
# 		dl = r['Data Label']
# 		cfgLevels = cat.split('+')
# 		cfgLevels.append(colno)
# 		cfgLevels.append(dl)
# 		print (cfgLevels)



def get_concept_insert_stmt(cPath, nodeName, projectId):
	return 'insert into i2b2demodata.concept_dimension ' \
    		+ '(concept_cd,concept_path,name_char,update_date,download_date,import_date,sourcesystem_cd) ' \
    		+ 'values (nextval(\'i2b2demodata.concept_id\'), \'' + cPath + '\', \'' + nodeName + '\',current_timestamp,current_timestamp,current_timestamp,' + '\'' + projectId + '\');'

def get_i2b2_insert_stmt(cPath, nodeName, dataType, projectId):
	levels = cPath.split('\\')
	try:
		hlevels = len(levels)-2   # account for begin and end \
	except:
		hlevels = 0

	baseCode = '00000000'  # TODO: need to get the concept_code from concept dimension

	#dataType = 'T'   # TODO: get the proper datatype

	xml = ''

	if dataType != 'T':
		xml = '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'

	cVisual = get_visual_node_level(hlevels)   # TODO this needs to figure out if path is a parent or a child node
	sql = 'insert into i2b2metadata.i2b2 (c_hlevel, c_fullname,c_name,c_visualattributes,c_synonym_cd,c_facttablecolumn,c_tablename,c_columnname,c_dimcode,c_tooltip,update_date,download_date' \
			+',import_date,sourcesystem_cd,c_basecode,c_operator,c_columndatatype,c_comment	,m_applied_path,c_metadataxml) values (' \
			+ str(hlevels) + ',\'' + cPath + '\', \'' + nodeName + '\', \'' + cVisual + '\',\'N\', \'CONCEPT_CD\', \'CONCEPT_DIMENSION\',\'CONCEPT_PATH\',' \
			+ '\'' + cPath + '\',' + '\'' + cPath + '\', current_timestamp, current_timestamp,current_timestamp,' + '\'' + projectId + '\', \'' + baseCode + '\',' \
			+ '\'LIKE\',\'' + dataType + '\', \'trial:'+ projectId + '\', \'@\',\'' + xml + '\');'

	return sql

# TODO this needs to figure out if path is a parent or a child node
def get_visual_node_level(hlevel):
	if hlevel > 2:
		return 'LA'
	return 'FA'

def main(cffile, dfile):

	#read_data_file(dfile)

	read_cfg_file(cffile)

	build_cf_levels()
	#parse_category()

	get_unique_codes(dfile)

	# if 	transmart:
	# 	print ('Building tranSMART cfg rows...')
	# 	r = build_transmart_rows(columns, afile)

	# 	print ('Writing config file...' + afile)		
	# 	write_transmart_cfg(r, afile)

	# else:
	# 	print ('Building TIES cfg rows...')
	# 	r = build_tiescfg_rows(columns)

	# 	print ('Writing config file...' + afile)
	# 	write_tiescfg(r, afile)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("cfgfile", help="Config file")
	parser.add_argument("datafile", help="Data file")
	parser.add_argument("-p", help="Project name")
#	parser.add_argument("-tab", action="store_true", help="tab delimited file (default comma)")
	args = parser.parse_args()

	if args.cfgfile:
		cfgfile = args.cfgfile
	if args.datafile:
		datafile = args.datafile
	if args.p:
		PROJECT = args.p	

	main(cfgfile, datafile)