import requests
import json
import argparse

term = ""
ontology = "SNOMEDCT"
BIOPORTAL_URL = 'http://data.bioontology.org/search'
BIOPORTAL_API_KEY = '75479e12-0267-4718-a7d7-2e5aed3b3ca4'

# see https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt
# T047|Disease or Syndrome, T191|Neoplastic Process
# T061|Therapeutic or Preventive Procedure
SEMANTIC_TYPES =  'T047, T191, T061'

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--term", help="Search term")
parser.add_argument("-o", "--onto", help="ontologies: SNOMEDCT   NCIT")
args = parser.parse_args()

if args.term:
	term =args.term
if args.onto:
	ontology = args.onto

url = BIOPORTAL_URL

querystring = {"q":term, "semantic_types":SEMANTIC_TYPES,"ontologies": ontology}  #SNOMEDCT   NCIT  ,"ontologies":"SNOMEDCT"

headers = {
    'authorization': "apikey token=" + BIOPORTAL_API_KEY,
    'cache-control': "no-cache",
    'postman-token': "e9e97c86-ecba-cca7-4213-3933e49f3e09"
}

response = requests.request("GET", url, headers=headers, params=querystring)

j = json.loads(response.text)

#print (j)
collect = j["collection"]

i = 0
found = False
suggestedTerms = {}
while ((not found) and (i < len(collect))):
    d = collect[i]
    try:
        for x in d['synonym']:
            if (is_ascii(x)):  # do this check because it contains weird char strings sometimes
                suggestedTerms.update({x:x})
    except Exception as e:
        try:
            pl = d['prefLabel']
            suggestedTerms.update({pl: pl})
        except Exception as e:
            pass  
        
    i += 1
keylist = suggestedTerms.keys()
#print(keylist)
sorted_list = sorted(set(keylist))

for t in sorted_list:
	print(t)