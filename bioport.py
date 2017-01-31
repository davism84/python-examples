import requests
import json
import argparse

term = ""

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--term", help="Search term")
args = parser.parse_args()

if args.term:
	term =args.term

url = "http://data.bioontology.org/search"

querystring = {"q":term}

headers = {
    'authorization': "apikey token=[ADD YOUR TOKEN HERE]",
    'cache-control': "no-cache",
    'postman-token': "e9e97c86-ecba-cca7-4213-3933e49f3e09"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

j = json.loads(response.text)

col = j["collection"]
print("Term:" + term)
print("Label: " + col[0]["prefLabel"])
#print(response.text)