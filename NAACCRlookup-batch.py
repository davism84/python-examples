import requests
import json
import argparse
import csv

headers = {
    'x-seerapi-key': "[ADD YOUR TOKEN HERE]",
    'cache-control': "no-cache",
    'postman-token': "55e9c632-3587-11a4-0a5c-a6d32378761f"
    }

f = open("ids.csv")
master_f = csv.reader(f)

for row in master_f:
    item = row[0]

    url = "https://api.seer.cancer.gov/rest/naaccr/16/item/" + str(item)

    response = requests.request("GET", url, headers=headers)

    if response.status_code == 200:
        html = json.loads(response.text)
        # get the documentation element
        desc = html['documentation']
        #print (desc.find('chap10-para'))
        print (str(item) + '-' + html['name'])
        # this will parse the description in the html
        for line in desc.split('\n'):
            if line.find('<div class=\'content chap10-para\'>') > -1:
                html_desc = line
                print (line)
    else:
        print (response.text)

f.close()



