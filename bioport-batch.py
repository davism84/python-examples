import requests
import json
import argparse
import csv

# search an array of dictionaries
def findterm(term, a):
	o = list(filter(lambda a:a['icd'] == term, a))
	if len(o) == 0:
		return False
	return True

term = ""
termCache = []
colhdr = ['code', 'desc', 'icd']
url = "http://data.bioontology.org/search"

querystring = {"q":term}

headers = {
    'authorization': "apikey token=[ADD YOUR TOKEN HERE]",
    'cache-control': "no-cache",
    'postman-token': "e9e97c86-ecba-cca7-4213-3933e49f3e09"
    }

print('Searching...')
with open("comorbid.csv") as csvfile:
	reader = csv.DictReader(csvfile)
	title = reader.fieldnames
	cnt = 0
	for row in reader:
		dec = ""
		for k,v in row.items():
			lastChar = v[-2:]  # just check last two chars
			val = v[:len(v)-2]

			if lastChar:
				if lastChar[-1:] == '0':   # if it is a zero only ignore it
					dec = "." +lastChar[0]  # add decimal to last 1 digit
				else:
					dec = "." + lastChar  # add decimal to last 2 digits
				if val:
					term = val + dec  # add decimal for ICD-9
					#if term not in termCache:
					r = findterm(term, termCache)
					#print(r)
					if r is False:		# if not in the cache then search
						querystring = {"q":term}
						response = requests.request("GET", url, headers=headers, params=querystring)   # search bioportal
						j = json.loads(response.text)
						col = j["collection"]
						if col:
							d = {'code': v, 'desc':col[0]["prefLabel"], 'icd':term}
							#print(d)
							termCache.append(d)
		#cnt = cnt+1
		#if cnt == 5:
		#	break
	print('building out csv...')
	with open("comorbid-out.csv", "w") as out:
		writer = csv.DictWriter(out, fieldnames=colhdr, dialect='excel', lineterminator='\n')
		writer.writeheader()
		for data in termCache:
			writer.writerow(data)



#print(response.text)