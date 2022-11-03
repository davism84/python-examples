import csv 
import json 
import argparse

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf) 

        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            jsonArray.append(row)
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
          


if __name__ == "__main__":
    datafile = ""
    title = ""
    output_file = ""

    parser = argparse.ArgumentParser()

    parser.add_argument("csvfile")
    parser.add_argument("jsonfile")

    args = parser.parse_args()

    if args.csvfile:
        csvFilePath = args.csvfile

    if args.jsonfile:
        jsonFilePath = args.jsonfile

    csv_to_json(csvFilePath, jsonFilePath)

