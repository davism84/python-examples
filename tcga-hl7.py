from lxml import etree
from lxml import objectify
import xml.etree.ElementTree as et
import os, csv
import sys
import re
import argparse
import requests
import unicodedata

headers = []
data = {}
config = False
cde = {}
disease = ""

def parseit(xmlfile):

	parser = etree.XMLParser(remove_blank_text=True)
	tree = etree.parse(xmlfile, parser)

	try:
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
		return True
	except Exception as e:
		return False

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

def writehl7(hlfile):

	if os.path.exists(hlfile):
		os.remove(hlfile)
	tcgafile = open(hlfile, 'w')

	# write MSH
	tcgafile.write(msh())
	tcgafile.write('\n')

	# write PID
	tcgafile.write(pid())
	tcgafile.write('\n')

	# write OBR
	tcgafile.write(obr())
	tcgafile.write('\n')

	# write OBX
	
	obxs = obx(data['bcr_patient_barcode'])   # get each line of obx as  single obx line
	if obx:
		for x in obxs:
			try:
				tcgafile.write(str(x))
				tcgafile.write('\n')
			except Exception:
				pass
#	tcgafile.write(obx2(data['bcr_patient_barcode']))  this was for a simple block of text

	tcgafile.close()

# HL7 message Header
def msh():
	return "MSH|^~&|Python||TCGA||||ORU^R01|Q89864576T91907826|P|2.3"

# HL7 Patient info
def pid():
	return "PID|1||" + data['bcr_patient_barcode'] + "||" + data['bcr_patient_barcode'] + "^" + \
		data['bcr_patient_barcode'] + "||20000101|"+data['gender'] +"||"+data['race'].title() +"||||||||||" + data['bcr_patient_barcode']

# HL7 Observation Request
def obr():
	return "OBR|||" + data['bcr_patient_barcode'] + "^TCGAReports^20180101|DIAGNOSIS:^PATHOLOGY|||201801081056|||||||||||||" + data['bcr_patient_barcode'] + "||201801081056|||F"

#HL7 Observation Results
def obx(whichId):
	reportText = []
	with open('../reports/report_manifest.txt', 'r') as csvfile:
		mline = csv.reader(csvfile, delimiter='\t')
		i = 0;
		for row in mline:
			if i > 0:
				file = row[1]
				parts = file.split(".")  # arse filename becauseit contains the tcgaid
				tcgaid = parts[0]  # part 0 is tcga id
				if tcgaid == whichId:
					#filePath = '../reports/' + row[0] + '/' + file.replace(".pdf", ".txt")
					filePath = '../converted/' + file + '.txt'
					print(filePath)
					if os.path.exists(filePath):
						with open(filePath, 'r', encoding="utf8") as rptFile:
							rline = csv.reader(rptFile)
							lineCnt = 1
							for rrow in rline:
								try:
									#print (rrow[0])
									rawText = rrow[0]
									#print(rawText)
									#scrubbedText = rawText.encode("utf-8")
									#print(type(scrubbedText))
									#scrubbedText = unicodedata.normalize('NFC', reportText)
									#scrubbedText = scrubbedText.replace('\"', '')
									#scrubbedText = scrubbedText.replace('\'', '')
									t = "OBX|" + str(lineCnt) + "||DIAGNOSIS:^PATHOLOGY||" + rawText + "||||||F|||201801090940"
									#print(t)
									reportText.append(t)
									lineCnt = lineCnt + 1
								except Exception as e:
									#print(e)
									pass
							rptFile.close()
							return reportText
			i = i + 1
		csvfile.close()
	return ""

def obx2(whichId):
	reportText = []
	with open('../reports/report_manifest.txt', 'r') as csvfile:
		mline = csv.reader(csvfile, delimiter='\t')
		i = 0;
		for row in mline:
			if i > 0:
				file = row[1]
				parts = file.split(".")  # arse filename becauseit contains the tcgaid
				tcgaid = parts[0]  # part 0 is tcga id
				if tcgaid == whichId:
					filePath = '../reports/' + row[0] + '/' + file.replace(".pdf", ".txt")
					if os.path.exists(filePath):
						rpt = open(filePath, 'r')
						rawText = rpt.read();						
						reportText = unicodedata.normalize('NFC', rawText)
						reportText = rawText.replace('\"', '')
						t = "OBX|1||DIAGNOSIS:^PATHOLOGY||" + str(reportText) + "||||||F|||201801090940"
						rpt.close()
						return t
			i = i + 1
		csvfile.close()
	return ""

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
#	parser.add_argument("xmlfile", help="a xml data file")
#	parser.add_argument("outfile", help="a out data file")
#	parser.add_argument("-conf", action="store_true", help="to generate just a config file")
	parser.add_argument("-disease", help="specify the disease (e.g., ov")

	args = parser.parse_args()

#	if args.xmlfile:
#		xmlfile =args.xmlfile
#	if args.outfile:
#		outfile =args.outfile
#	if args.conf:
#		config = True
	if args.disease:
		disease = args.disease

	errorList = []

	# read the clinical manifest file to use as the master list
	with open('MANIFEST.txt', 'r') as tsvfile:
		mline = csv.reader(tsvfile, delimiter='\t')
		i = 0;
		for row in mline:
			if i > 0:
				xmlfile = row[1]
				parts = xmlfile.split("/") # break into parts to get filename
				if xmlfile.find(".xml") > -1:
					xdir = parts[0]
					xfile = parts[1]
					outfile = xfile.replace('.xml', '.hl7')

					print (xdir + " -- " + xfile + " ==> " + outfile)
					if parseit(xdir + '/' + xfile):
						print('Writing Output file...')
						#writehl7(xdir + '/' + outfile)
						writehl7('../converted/hl7/' + outfile)
					else:
						errorList.append(xdir + '/' + xfile)
			i = i + 1
	tsvfile.close()
	if len(errorList) > 0:
		print('Exception list')
		print('--------------')
		for e in errorList:
			print(e)