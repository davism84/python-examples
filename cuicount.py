import json
import csv

colhdr = ['code']

cList = []

with open("ties-train-concepts.json", 'r') as f:
	data = json.load(f)

i = 0
while i < len(data):
	r = data[i]

	tid = r["TCGA_ID"]
	concepts = r["CONCEPT_CODE_SET"]
	conceptList = concepts.split("\n")

	cnt = len(conceptList)

	print("tcgid: " + tid + " concepts cnt: " + str(cnt))
	#print(cnt)

	i = i + 1
