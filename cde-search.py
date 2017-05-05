from lxml import etree
from lxml import html
import xml.etree.ElementTree as et
import os
import sys
import re
import argparse
import requests

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
	print(response.status_code)
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
				metadata = {'label': label, 'definition': definition}			
		except Exception as e:
			print(e)
			raise e

	return metadata

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("cde", help="cde public id")
	args = parser.parse_args()
	if args.cde:
		publicId = args.cde
	try:
		cde = getCDEMeta(publicId)
		print(cde['label'], cde['definition'])
	except Exception as e:
		print('Unknown CDE public id ' + str(publicId))
