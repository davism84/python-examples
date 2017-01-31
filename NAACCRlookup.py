import requests
import json
import argparse

item = 0
verbose = False

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--item", help="NAACCR Item Code")
parser.add_argument("-v","--verbose", action="store_true", help="display verbose output")
args = parser.parse_args()

if args.verbose:
  verbose = True
if args.item:
    item =args.item

url = "https://api.seer.cancer.gov/rest/naaccr/16/item/" + str(item)

headers = {
    'x-seerapi-key': "[ADD YOUR TOKEN HERE]",
    'cache-control': "no-cache",
    'postman-token': "55e9c632-3587-11a4-0a5c-a6d32378761f"
    }

response = requests.request("GET", url, headers=headers)
#print (response.status_code)

if response.status_code == 200:
    html = json.loads(response.text)
    if verbose:
        print (html['name'])
        print (html['documentation'])
    # get the documentation element
    desc = html['documentation']
    #print (desc.find('chap10-para'))
    print (html['name'])
    # this will parse the description in the html
    for line in desc.split('\n'):
        if line.find('<div class=\'content chap10-para\'>') > -1:
            html_desc = line
            print (line)
else:
    print (response.text)




