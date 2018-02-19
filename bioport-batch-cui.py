import requests
import json
import argparse
import csv

# this program reads in a simple list of CUIs then looks them up in BioPortal
term = ""
termCache = []
colhdr = ['code', 'name']
url = "http://data.bioontology.org/search"

querystring = {"q":term}

headers = {
    'authorization': "apikey token=75479e12-0267-4718-a7d7-2e5aed3b3ca4",
    'cache-control': "no-cache",
    'postman-token': "e9e97c86-ecba-cca7-4213-3933e49f3e09"
    }

print('Searching...')
reader = open("conceptlist.csv", "r")
cnt = 0
for term in reader:
	querystring = {"q":term}
	response = requests.request("GET", url, headers=headers, params=querystring)   # search bioportal
	j = json.loads(response.text)
	try:
		col = j["collection"]
		if col:
			name = col[0]["prefLabel"]
			if not name:
				name = "[Unable to determine]"
			d = {'code': col[0]["cui"][0], 'name':name}
			#print(d)
			termCache.append(d)
	except:
		continue
print('building out csv...')
with open("conceptlist-bioportal.csv", "w") as out:
	writer = csv.DictWriter(out, fieldnames=colhdr, dialect='excel', lineterminator='\n')
	writer.writeheader()
	for data in termCache:
		writer.writerow(data)



#print(response.text)