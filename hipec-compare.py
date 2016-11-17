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
import datetime

verbose = False
masterfile = 'data/HIPEC-Master.csv'
carefile = ''
#redcapfile = 'redcap-import.csv'  #default if no options are specified
matchfile = 'data/return-hipec-{timestamp}.csv'.format(timestamp=datetime.date.today())
newhipecfile = 'data/new-hipec-{timestamp}.csv'.format(timestamp=datetime.date.today())
masterupdatefile = 'data/update-master-{timestamp}.csv'.format(timestamp=datetime.date.today())

parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", action="store_true", help="display verbose output")
parser.add_argument("-n", "--newfile", help="CARe file with new HIPECs")
#parser.add_argument("-m","--master", help="master patient file")
#parser.add_argument("-o", "--redcapfile", help="output file [OPTIONAL]; default will be: redcap-import.csv")
#parser.add_argument("-c", "--match", action="store_true", help="create a file with matched patient against master")
args = parser.parse_args()

if args.verbose:
    verbose = True
if args.newfile:
    carefile = args.newfile

# if args.match:
#   match = True
# if args.master:
#     masterfile = args.master
# if args.redcapfile:
#     redcapfile = args.redcapfile
        
# read in the master file list
f = open(masterfile)
master_f = csv.reader(f)

masterLastNames = []
masterIds = []
masterMRNs = {}
masterHash = {}
newpatDateHash = {}
newpatIdHash = {}

next(master_f)  # skip the header
for row in master_f:
  fullname = row[1] + ',' + row[2] + ',' + row[3]
  masterLastNames.append(fullname)
  masterIds.append(int(row[0]))   # convert the ids to int
  masterHash[fullname] = int(row[0])  # store ids in a hash to quickly get it later
  masterMRNs[fullname] = row[4] + ',' + row[5]  # get epic and mrn
f.close()

# read in the CARE file
f = open(carefile)
new_f = csv.reader(f)

newLastNames = []
next(new_f)  # skip the header
for row in new_f:
    fullname = row[0].strip().upper() + ',' + row[1].strip().upper() + ',' + row[2].strip()

    if len(fullname) > 2:
        newLastNames.append(fullname)
        newpatIdHash[fullname] = row[3] + ',' + row[4]  # get epic and mrn # store the mrn
        #newpatDateHash[fullname] = row[0]  # store the date
f.close()

# change these to sets
masterSet = set(masterLastNames)
newSet = set(newLastNames)

# determine the difference, create a new set
patientDiff = newSet.difference(masterSet)
# get intersections
matchingPatients = masterSet.intersection(newSet)

# convert set to a list
patientDiffList = list(patientDiff)

# get the next available id
newId = max(masterIds) + 1
newfilelist = []

# Create the NEW-HIPEC.CSV file for import into redcap
with open(newhipecfile, 'w') as csvfile:
  try:
    fieldnames = ['study_id', 'last_name', 'first_name', 'dob', 'registry_complete', 'redcap_event_name', 'gender', 'race']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel', lineterminator='\n')
    writer.writeheader()
  
    # loop through to create a new CSV file to import
    for index in range(len(patientDiffList)):
      patient = patientDiffList[index].split(',')
      n = patient[0].strip().upper() + ',' + patient[1].strip().upper() + ',' + patient[2].strip()
      mrn = newpatIdHash[n]
      outstr = str(newId) + ',' + n + ',' + str(mrn)
      newfilelist.append(outstr)
      if patient:
        writer.writerow({'study_id': str(newId), 'last_name': patient[0].strip().upper(), 'first_name': patient[1].strip().upper(),
                         'dob': patient[2].strip(), 'registry_complete':'0', 'redcap_event_name': 'registry_arm_1'})
        newId = newId + 1
  finally:
      csvfile.close()
      

# sort the new list so in decending order by redcap id
newsortedlist = sorted(newfilelist)

# write out the file to update new matches file
o = open(masterupdatefile, 'w')
for idx in range(len(newsortedlist)):
	o.write(newsortedlist[idx])
	o.write('\n')

o.close()


# write out the existing patients to a separate file
matches = list(matchingPatients)
with open(matchfile, 'w') as csvfilematch:
    try:
        fieldnames = ['hipec_id', 'last_name', 'first_name', 'dob', 'epic_mrn', 'medipac_mrn']
        writer = csv.DictWriter(csvfilematch, fieldnames=fieldnames, dialect='excel', lineterminator='\n')
        writer.writeheader()

        for index in range(len(matchingPatients)):
            fn = matches[index]
            pat = fn.split(',')
            pid = masterHash[fn]
            mrns = masterMRNs[fn]
            toks = mrns.split(',')
            #hipecdt = newpatDateHash[fn]  # get the hipec date, this came over with new info
            writer.writerow({'hipec_id': str(pid), 'last_name': pat[0].strip().upper(), 'first_name': pat[1].strip().upper(),
                             'dob': pat[2].strip(), 'epic_mrn': toks[0], 'medipac_mrn': toks[1]})
    finally:
        csvfilematch.close()

print("Done.")
