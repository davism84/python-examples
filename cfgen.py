# program to generate a CFG for TIES from a set of CSV filesfor Cancer Registry
import argparse
import csv
import requests
import json
import re
import string

NAMESPACE = "cancer-reg"
KEYNS = "ties.model"
TAG_RE = re.compile(r'<[^>]+>')
transmart = False
delimiter = ','
outfilename = "config.cfg"

def remove_tags(text):
	tmp = TAG_RE.sub('', text)
	scrubbed = tmp.replace('&#8217;', '')
	return scrubbed

columns = ""
metadata = []
headers = {
	'x-seerapi-key': "bc70251be6e4dad034b029dcb199e6f5",'cache-control': "no-cache",
	'postman-token': "55e9c632-3587-11a4-0a5c-a6d32378761f"
	}

# read the data csv file
def read_data(afile):
	with open(afile) as csvfile:
		reader = csv.DictReader(csvfile, delimiter=delimiter)
		cols = reader.fieldnames

		#print(cols)

	csvfile.close()
	return cols

def read_template():
	tmpl = open('template.cfg', 'r')

	cfgheader = tmpl.read()

	tmpl.close()
	return cfgheader

def read_metadata():
	with open("IPM-CR-CDE-DataDictionary.csv") as csvfile:
		reader = csv.DictReader(csvfile)
		cols = reader.fieldnames

		for row in reader:
			#print(row)
			metadata.extend([{cols[i]:row[cols[i]] for i in range(len(cols))}])

	csvfile.close()

# match against metadata, returning phi, naa code
def match(field):
	phi = ""
	naa = ""
	dt = "CATEGORY"
	pk = ""
	cat = ""
	mlabel = ""
	for r in metadata:
		#print(type(r))
		fld = r['FieldName'].upper()
		if field == fld:
			mlabel = r['METRIQLabel']
			phi = r['PHI']
			naa = r['NAACCRCode']
			dt = r['DataType']
			pk = r['PK']
			cat = r['DeepPheModel']  #METRIQFolder
			if len(dt) < 1:
				dt = 'CATEGORY'  # default to category
			break
	return phi, naa, dt, pk, cat,mlabel

# write a configuration file
def write_tiescfg(rows, afile):
	outfile = outfilename + ".cfg"
	o = open(outfile, 'w')

	o.write(read_template()) # read template header
	for r in rows:
		#print (r)
		o.write(r)
	o.close()

def getNAACCR(item):
	url = "https://api.seer.cancer.gov/rest/naaccr/16/item/" + str(item)
	response = requests.request("GET", url, headers=headers)
	descript = ""
	name = ""
	#print (response.status_code)
	if response.status_code == 200:
		html = json.loads(response.text)
		# get the documentation element
		desc = html['documentation']
		name = html['name']
		#print(desc)
		# this will parse the description in the htm
		for line in desc.split('\n'):
			if line.find('<div class=\'content chap10-para\'>') > -1:
				html_desc = line
				#print(html_desc)
				descript = remove_tags(html_desc)
				return name, descript
	else:
		print (item + ' Not found  ')

	return name, descript

def build_tiescfg_rows(columns):
	row = []
	phi = ""
	prettyName = ""
	descript = ""
	dataType = ""
	pk = ""
	ns = ""
	mlabel = ""
	# for each column get metadata and naaccr info
	for col in columns:
		p, ncode, dataType, pk, mlabel = match(col)
		if pk:
			ns = KEYNS
		else:
			ns = NAMESPACE

		#print(ncode)
		if p:
			phi = "true"
		else:
			phi = "false"
		#print (ncode)
		if ncode:
			prettyName, descript = getNAACCR(ncode)
			#print ("w/ code: " + prettyName + "  " + descript)
			if len(prettyName) < 1:
				prettyName = mlabel
				descript = ""
			#print (name + ":" + descript )
		else:
			prettyName = mlabel
		
		line =  "true\t|true\t|" + phi + "\t|" + "false	|true	|" + ns + "\t|" + col.lower() + "\t|" + dataType + "\t|\t\t|" + prettyName + "\t|" +descript + "\n"
		row.append(line)
		prettyName = ""
		descript = ""
		#print(line)
	#print (row)
	return row

def build_transmart_rows(columns, afile):
	row = []
	phi = ""
	prettyName = ""
	descript = ""
	dataType = ""
	pk = ""
	ns = ""
	cat = ""
	cnt = 1
	mlabel = ""
	# for each column get metadata and naaccr info
	for col in columns:
		p, ncode, dataType, pk, cat, mlabel = match(col)
		if pk:
			ns = KEYNS
		else:
			ns = NAMESPACE

		#print(ncode)
		if p:
			phi = "true"
		else:
			phi = "false"
		#print (ncode)
		if ncode:
			prettyName, descript = getNAACCR(ncode)
			#print ("w/ code: " + prettyName + "  " + descript)
			if len(prettyName) < 1:
				prettyName = mlabel
				descript = ""
			#print (name + ":" + descript )
		else:
			if mlabel:
				prettyName = mlabel
			else:
				prettyName = col

			#print ('No code using ' + mlabel)

		if len(cat) == 0:
			cat = 'Other'
		
		#line =  {afile + "\tFolder\t" + str(cnt) + "\t" + prettyName + "\t" + col.lower() + "\t" + ncode}
		line =  {'Filename': afile, 'Category Code':cat, 'Column Number': str(cnt),  'Data Label': prettyName} #, 'Data Label Source':col.lower(), 'Controlled Vocab Code': ncode}
		row.append(line)
		cnt += 1
		prettyName = ""
		descript = ""
		#print(line)
	#print (row)
	return row

def write_transmart_cfg(rows, afile):

	headers = ['Filename', 'Category Code', 'Column Number', 'Data Label']  #, 'Data Label Source', 'Controlled Vocab Code']
	afile = outfilename # + "-transcfg.tsv"

	with open(afile, "w") as out:
		writer = csv.DictWriter(out, fieldnames=headers, dialect='excel', lineterminator='\n', delimiter='\t')
		writer.writeheader()
		for data in rows:
			writer.writerow(data)

def main(afile):
	print ('Reading data file headers...')
	# read data file to get column header to use to generate 
	columns = read_data(afile)
	
	print ('Reading metadata...')
	# read the file where metadata is maintained
	read_metadata()

	if 	transmart:
		print ('Building tranSMART cfg rows...')
		r = build_transmart_rows(columns, afile)

		print ('Writing config file...' + afile)		
		write_transmart_cfg(r, afile)

	else:
		print ('Building TIES cfg rows...')
		r = build_tiescfg_rows(columns)

		print ('Writing config file...' + afile)
		write_tiescfg(r, afile)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("afile", help="a csv data file")
	parser.add_argument("-ns", help="namespace")
	parser.add_argument("-o", help="output file")
	parser.add_argument("-tab", action="store_true", help="tab delimited file (default comma)")
	parser.add_argument("-transmart", action="store_true", help="generate a tranSMART configuration file")	
	args = parser.parse_args()

	if args.afile:
		afile =args.afile
	if args.ns:
		NAMESPACE = args.ns
	if args.transmart:
		transmart = True
	if args.tab:
		delimiter = '\t'
	if args.o:
		outfilename = args.o

	main(afile)