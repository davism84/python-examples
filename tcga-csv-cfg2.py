from lxml import etree
from lxml import objectify
import xml.etree.ElementTree as et
import os, csv
import sys
import re
import argparse
import requests

headers = []
data = {}
config = False
cde = {}
disease = ""

def parseit(xmlfile):

	try:
		parser = etree.XMLParser(remove_blank_text=True)
		tree = etree.parse(xmlfile, parser)

		root = tree.getroot()
		patRoot = '{' + root.nsmap[disease] + '}patient'
		adminRoot = root.nsmap['admin']
		topRoot = 'tcga_bcr'
		nteRoot = '{' + root.nsmap[disease + '_nte'] + '}'
		nte = '{' + root.nsmap['nte'] + '}'
		for elem in root.iter():
		#if not hasattr(elem.tag, 'find'): continue  # (1)
		#i = elem.tag.find('}')
		#print(elem.tag)
			itop = elem.tag.find(topRoot)
			iadmin = elem.tag.find(adminRoot)
			ipatroot = elem.tag.find(patRoot)
			inteRoot = elem.tag.find(nteRoot)
			inte = elem.tag.find(nte)
			if itop < 0 and iadmin < 0 and ipatroot < 0 and inteRoot < 0 and inte < 0:    # skip the tcga_bcr and admin levels
				i = elem.tag.find('}')
				elem.tag = elem.tag[i+1:]
				if elem.tag and elem.text:
				#print (elem.tag  + " " + elem.text )
					headers.append(elem.tag)
					try:
						pubid = elem.attrib['cde']
					except Exception as e:
						pubid = None
				
					if elem.text.find(',') > 0:
						et = '"' + elem.text + '"'
					else:
						et = elem.text

					data[elem.tag] = et
					cde[elem.tag] = pubid
	# add a tag for this disease
		headers.append('tag')
		return True
	except Exception as e:
		return False

def writecsv(csvfile):

	if os.path.exists(csvfile):
		os.remove(csvfile)
	tcgafile = open(csvfile, 'w')

	# get the key field first
	for k in headers:
		if k == 'bcr_patient_barcode':  # consider this like an MRN for TCGA
			tcgafile.write('SSN,')	  # generate a fake SSN
	for h in headers:
		if h != 'bcr_patient_barcode':  # consider this like an MRN for TCGA
			tcgafile.write(h + ',')
	tcgafile.write('\n')
	for k2 in headers:  # write out only the fake SSN
		try:
			if k2 == 'bcr_patient_barcode':  # consider this like an MRN for TCGA
				tcgafile.write(data[k2] + ',')
		except Exception as e:
			pass		
	for d in headers:
		try:
			if d != 'bcr_patient_barcode':  # consider this like an MRN for TCGA
				tcgafile.write(data[d] + ',')
		except Exception as e:
			pass

	# add a disease tag field to tag the data
	tcgafile.write(disease)
	tcgafile.write('\n')
	tcgafile.close()

def writecfg(csvfile):
	if os.path.exists(csvfile):
		os.remove(csvfile)
	tcgafile = open(csvfile, 'w')
	# read static header
	tcgafile.write(read_template())
	tcgafile.write('\n')
	i = 1
	dataType = 'CATEGORY'
	ns = 'tcga.clinical.patient'
	for k in headers:  # get the fake SSN first
		if k == 'bcr_patient_barcode':  # consider this like an MRN for TCGA
			ns = 'ties.model'
			h = 'ssn'
			phi = 'true'
			definition = " "
			line =  "true|true|"+str(phi) +"|false	|true	|" + ns + "| 1 | " + h + "| 0| " + dataType + "| | Patient ID|" + definition
			tcgafile.write(line)
			tcgafile.write('\n')

	for h in headers:
		try:
			pubid = cde[h]
		except Exception as e:
			pass
		label = ''
		if pubid:
			#print('Searching for ' + str(pubid))
			cdemeta = getCDEMeta(pubid)
			if cdemeta:
				label = cdemeta['label']
				definition = cdemeta['definition']
				if label:
					#line = label + ',' + h +',,' + pubid  # pretty name + the field name
					if h == 'tag':
						ns = 'ties.model'
						phi = 'false'
						definition = " "
						h = 'tag'
					else:
						ns = 'tcga.clinical.patient'
						phi = 'false'
					if h != 'bcr_patient_barcode':  # only write out non patient key fields
						line =  "true|true|"+str(phi) +"|false	|true	|" + ns + "| 1 | " + h + "|" +str(i) + "| " + dataType + "| |" + label + "|" + definition
						tcgafile.write(line)
						tcgafile.write('\n')
						i = i + 1
	tcgafile.close()

def read_template():
	tmpl = open('../template.cfg', 'r')

	cfgheader = tmpl.read()

	tmpl.close()
	return cfgheader

def getCDEMeta(cde):
	metadata = {}
	label = ''
	definition = ''

	url = "https://cadsrapi.nci.nih.gov/cadsrapi4/GetXML"

	querystring = {"query":"DataElement[@publicId=" + cde + "]"}

	xheaders = {
    	'cache-control': "no-cache",
    	'postman-token': "c50954f2-0591-a421-66ed-e969866bf5a4"
    }
	response = requests.request("GET", url, headers=xheaders, params=querystring)
	if response.status_code == 200:

		# must use this because CDE api gives it back XML as a string with unicode (lxml doesn't support it)
		try:
			root = et.fromstring(response.text)
			for x in root.iter():
				if x.tag == 'field':
					if x.attrib['name'] == 'longName':
						label = x.text
					if x.attrib['name'] == 'preferredDefinition':
						definition = x.text

			if label and definition:
				metadata = {'label': label, 'definition': definition, 'cde':cde}
		except Exception as e:
			print(e)
			raise e

	return metadata

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
#	parser.add_argument("xmlfile", help="a xml data file")
#	parser.add_argument("csvfile", help="a csv data file")
#	parser.add_argument("-conf", action="store_true", help="to generate just a config file")
	parser.add_argument("-disease", help="specify the disease (e.g., ov")

	args = parser.parse_args()

#	if args.xmlfile:
#		xmlfile =args.xmlfile
#	if args.csvfile:
#		csvfile =args.csvfile
#	if args.conf:
#		config = True
	if args.disease:
		disease = args.disease

#	parseit(xmlfile)

#	if config == True:
#	cfgfile = csvfile[0:-3] + 'cfg'
#	print('Writing conf file...' + cfgfile)
#	writecfg(cfgfile)
	#print('Writing CSV file...' + csvfile)
#	writecsv(csvfile)


	errorList = []

	# read the clinical manifest file to use as the master list
	with open('MANIFEST.txt', 'r') as tsvfile:
		mline = csv.reader(tsvfile, delimiter='\t')
		i = 0;
		for row in mline:
			headers = []
			data = {}
			cde = {}
			if i > 0:
				xmlfile = row[1]
				parts = xmlfile.split("/") # break into parts to get filename
				if xmlfile.find(".xml") > -1:
					xdir = parts[0]
					xfile = parts[1]
					cfgfile = xfile.replace('.xml', '.cfg')
					csvfile = xfile.replace('.xml', '.csv')
					xinfile = xdir + '/' + xfile

					if parseit(xinfile):
						print('Writing conf file...' + cfgfile)
						writecfg(cfgfile)
						print('Writing CSV file...')
						#writehl7(xdir + '/' + outfile)
						writecsv('../converted/csv/' + csvfile)
					else:
						print("Not found " + xinfile)
						errorList.append(xinfile)
			i = i + 1
	tsvfile.close()
	if len(errorList) > 0:
		print('Exception list')
		print('--------------')
		for e in errorList:
			print(e)