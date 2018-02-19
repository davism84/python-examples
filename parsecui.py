import json
import csv

colhdr = ['code']

cList = []

with open("ties-train-concepts.json", 'r') as f:
	data = json.load(f)

i = 0
while i < len(data):
	r = data[i]

	concepts = r["CONCEPT_CODE_SET"]
	if concepts:
		conceptList = concepts.split("\n")

		for c in conceptList:
			try:
				cui = c[1:len(c)]   # take off the first char
				try:
					cList.index(cui)
				except:
					cList.append(cui)
			except:
				continue
	i = i + 1

print ("unique concepts")
print (cList)


print('building out concepts...')
f = open("conceptlist.csv", "w")
for row in cList:
	f.write(row)
	f.write("\n")

f.close()