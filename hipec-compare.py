###############################################################################
# hipec-compare.py
#
# this program is used to read a master file list and new patient file list
# to determine the newly added patients, then generate a CSV file to 
# import into the REDCap HIPEC database
#
# prereqs:  python must be installed  (https://www.python.org/downloads/)
# for usage: python hipec-compare.py -h
###############################################################################
import csv
import argparse
import re

verbose = False
match = False
masterfile = ''
newfile = ''
outfile = 'redcap-import.csv'  #default if no options are specified
matchfile = 'matches.log'

parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", action="store_true", help="display verbose output")
parser.add_argument("-m","--master", help="master patient file")
parser.add_argument("-n", "--newfile", help="new patient file")
parser.add_argument("-o", "--outfile", help="output file [OPTIONAL]; default will be: redcap-import.csv")
parser.add_argument("-c", "--match", action="store_true", help="create a file with matched patient against master")
args = parser.parse_args()

if args.verbose:
  verbose = True
if args.match:
  match = True  
if args.master:
    masterfile = args.master
if args.newfile:
    newfile = args.newfile
if args.outfile:
    outfile = args.outfile
        
# read in the master file list
f = open(masterfile)
master_f = csv.reader(f)

masterLastNames = []
masterIds = []
next(master_f)  # skip the header
for row in master_f:
  fullname = row[1] + ', ' + row[2] + ', ' + row[3]
  masterLastNames.append(fullname)
  masterIds.append(int(row[0]))   # convert the ids to int

f.close()

# read in the new master file list
f = open(newfile)
new_f = csv.reader(f)

newLastNames = []
next(new_f)
for row in new_f:
  fullname = row[2] + ', ' + row[3] + ', ' + row[4]
  newLastNames.append(fullname)

f.close()

# change these to sets
masterSet = set(masterLastNames)
newSet = set(newLastNames)

# determine the difference, create a new set
patientDiff = newSet.difference(masterSet)
# get intersections
matchingPatients = masterSet.intersection(newSet)

if verbose:
  print (patientDiff)
  print (masterLastNames[1])
  print (newLastNames[1])
  print (max(masterIds))
  print ("These patients already existing...")
  print (existPatients)
  
# convert set to a list 
patientDiffList = list(patientDiff)

# get the next available id
newId = max(masterIds) + 1

# out file
with open(outfile, 'w') as csvfile:
  try:
    fieldnames = ['study_id', 'last_name', 'first_name', 'dob', 'registry_complete', 'redcap_event_name', 'gender', 'race']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel', lineterminator='\n')
    writer.writeheader()
  
    # loop through to create a new CSV file to import
    for index in range(len(patientDiffList)):
      patient = patientDiffList[index].split(',')
      if verbose:
        print (patient)
      writer.writerow({'study_id': str(newId), 'last_name': patient[0].strip().upper(), 'first_name': patient[1].strip().upper(), 'dob': patient[2].strip(), 'registry_complete':'0', 'redcap_event_name': 'registry_arm_1'})
      newId = newId + 1
  finally:
      csvfile.close();

# write out the existing patients to a separate file
if match:
  matches = list(matchingPatients)
  log = open(matchfile, 'w')
  log.write('The following patients already exist in the HIPEC DB:' + newfile)
  log.write('\n')
  for index in range(len(matchingPatients)):
    log.write(matches[index])
    log.write('\n')
  log.close()    
    
print("Success -> " + outfile)
