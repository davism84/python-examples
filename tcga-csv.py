from lxml import etree
from lxml import objectify
import xml.etree.ElementTree as et
import os
import sys
import re
import argparse
import requests

headers = []
data = {}
config = False
cde = {}
def parseit(xmlfile):

	parser = etree.XMLParser(remove_blank_text=True)
	tree = etree.parse(xmlfile, parser)

	root = tree.getroot()
	patRoot = '{' + root.nsmap['skcm'] + '}patient'
	adminRoot = root.nsmap['admin']
	topRoot = 'tcga_bcr'
	nteRoot = '{' + root.nsmap['skcm_nte'] + '}'
	nte = '{' + root.nsmap['nte'] + '}'
	for elem in root.iter():
		#if not hasattr(elem.tag, 'find'): continue  # (1)
		#i = elem.tag.find('}')
		print(elem.tag)
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

def writecsv(csvfile):

	if os.path.exists(csvfile):
		os.remove(csvfile)
	tcgafile = open(csvfile, 'w')
	for h in headers:
		tcgafile.write(h + ',')
	tcgafile.write('\n')
	for d in headers:
		tcgafile.write(data[d] + ',')
	tcgafile.write('\n')
	tcgafile.close()

def writecfg(csvfile):
	if os.path.exists(csvfile):
		os.remove(csvfile)
	tcgafile = open(csvfile, 'w')
	i = 1
	for h in headers:
		pubid = cde[h]
		label = ''
		if pubid:
			cdemeta = getCDEMeta(pubid)
			if cdemeta:
				label = cdemeta['label']
				if label:
					line = label + ',' + h +',,' + pubid  # pretty name + the field name
					tcgafile.write(line)
					tcgafile.write('\n')
					i = i + 1

	tcgafile.close()	

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
	parser.add_argument("xmlfile", help="a xml data file")
	parser.add_argument("csvfile", help="a csv data file")
	parser.add_argument("-conf", action="store_true", help="config")

	args = parser.parse_args()

	if args.xmlfile:
		xmlfile =args.xmlfile
	if args.csvfile:
		csvfile =args.csvfile
	if args.conf:
		config = True

	parseit(xmlfile)

	if config == True:
		writecfg(csvfile)
	else:
		print('Writing CSV file...')
		writecsv(csvfile)
