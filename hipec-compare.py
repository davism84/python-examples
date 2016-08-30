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
redcapfile = 'redcap-import.csv'  #default if no options are specified
matchfile = 'existing-matches.csv'
newmatchfile = 'new-patients.csv'

parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", action="store_true", help="display verbose output")
parser.add_argument("-m","--master", help="master patient file")
parser.add_argument("-n", "--newfile", help="new patient file")
parser.add_argument("-o", "--redcapfile", help="output file [OPTIONAL]; default will be: redcap-import.csv")
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
if args.redcapfile:
    redcapfile = args.redcapfile
        
# read in the master file list
f = open(masterfile)
master_f = csv.reader(f)

masterLastNames = []
masterIds = []
masterHash = {}
newpatDateHash = {}
newpatIdHash = {}

next(master_f)  # skip the header
for row in master_f:
  fullname = row[1] + ',' + row[2] + ',' + row[3]
  masterLastNames.append(fullname)
  masterIds.append(int(row[0]))   # convert the ids to int
  masterHash[fullname] = int(row[0])  # store ids in a hash to quickly get it later
f.close()

# read in the new patient file list
f = open(newfile)
new_f = csv.reader(f)

newLastNames = []
next(new_f)  # skip the header
for row in new_f:
	fullname = row[2].strip().upper() + ',' + row[3].strip().upper() + ',' + row[4].strip()
	if verbose:
		fullname
  	newLastNames.append(fullname)
	newpatDateHash[fullname] = row[0]  # store the date
	newpatIdHash[fullname] = row[1]  # store the mrn
f.close()

# change these to sets
masterSet = set(masterLastNames)
newSet = set(newLastNames)

# determine the difference, create a new set
patientDiff = newSet.difference(masterSet)
# get intersections
matchingPatients = masterSet.intersection(newSet)

#if verbose:
#	print (newpatIdHash.keys())
#  print (patientDiff)
#  print (masterLastNames[1])
#  print (newLastNames[1])
#  print (max(masterIds))
  


# convert set to a list 
patientDiffList = list(patientDiff)

# get the next available id
newId = max(masterIds) + 1
newfilelist = []

# redcap import file
with open(redcapfile, 'w') as csvfile:
  try:
    fieldnames = ['study_id', 'last_name', 'first_name', 'dob', 'registry_complete', 'redcap_event_name', 'gender', 'race']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel', lineterminator='\n')
    writer.writeheader()
  
    # loop through to create a new CSV file to import
    for index in range(len(patientDiffList)):
      patient = patientDiffList[index].split(',')
      n = patient[0].strip().upper() + ',' + patient[1].strip().upper() + ',' + patient[2].strip()
      #if verbose:
      #	print (n)
      mrn = newpatIdHash[n]      
      hipecdt = newpatDateHash[n]
      outstr = str(newId) +',' + n + ','+ str(mrn) + ',,,,' + hipecdt
      if verbose:
      	print ('outstr: ' + outstr)
      newfilelist.append(outstr)  # save the new record with ids

      #if verbose:
      #  print (newfilelist)
      writer.writerow({'study_id': str(newId), 'last_name': patient[0].strip().upper(), 'first_name': patient[1].strip().upper(), 'dob': patient[2].strip(), 'registry_complete':'0', 'redcap_event_name': 'registry_arm_1'})    
      newId = newId + 1
  finally:
      csvfile.close()
      

# sort the new list so in decending order by redcap id
newsortedlist = sorted(newfilelist)

# write out the new matches file
o = open(newmatchfile, 'w')
for idx in range(len(newsortedlist)):
	o.write(newsortedlist[idx])
	o.write('\n')

o.close()


# write out the existing patients to a separate file
if match:
  matches = list(matchingPatients)
  log = open(matchfile, 'w')
  log.write('The following patients already exist in the HIPEC DB:' + newfile)
  log.write('\n')
  log.write('redcap_id,last_name,first_name,dob,hipec_date')
  log.write('\n')  
  for index in range(len(matchingPatients)):
  	fn = matches[index]
  	pid = masterHash[fn]
	hipecdt = newpatDateHash[fn]  # get the hipec date, this came over with new info
	log.write(str(pid) + ',' + fn +',' + hipecdt)
	log.write('\n')
  log.close()    
    
print("Success -> " + redcapfile)
