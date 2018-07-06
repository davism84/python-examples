# general program to add multi instance data into REDCAP
# usage is just to pipe the data into a file, it uses simple print statements
from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import argparse

infile = ""
redcapForm = ""

def process():

#	print("record_id,redcap_repeat_instrument,redcap_repeat_instance,tumor_seq_no,histology_subtype,primary_site,path_t,path_n,path_m")
	# read csv
	df1 = pd.read_csv(infile)
	df = df1.drop_duplicates()
	pids = df['record_id'].unique()
	gb = df.groupby('record_id')
	#muts = gb.groups
	colHdr =df1.columns
	cidx = 1
	header = "record_id,redcap_repeat_instrument,redcap_repeat_instance"  # default for multi instance forms
	for c in colHdr:
		if cidx > 1:  # skip the first column which is assunmed to be record_id
			header = header + ',' + c 
		cidx = cidx + 1
	print(header)

#	print("----------------------------------------------")
#	print("File: " + infile)
#	print("Total Rows: " + str(df['ID'].count()) + " Columns: " + str(len(df.columns)))

	unqIds = df
	lastId = 0
	lastType = ""
	instance = 1
	for pid in pids:

		# go get a specific group
		grp = gb.get_group(pid)

		instance = 1
		# then  to iterate over it
		for i,r in grp.iterrows():
			# what columns are there

			colHdr =grp.columns
			data = ""
			cidx = 1
			for c in colHdr:
				if cidx > 1:  # skip the first column which is assunmed to be record_id
					data = data + str(r[c]) + ','
				cidx = cidx + 1				
				#print(data)
			data = data[:-1]  # trunc the ending  comma		
			record = str(pid) + ',' + redcapForm + ',' + str(instance) + ',' + data
			instance = instance + 1
			print(record)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("infile", help="Data file to merge")
	parser.add_argument("-f", help="redcap form")
	args = parser.parse_args()

	if args.infile:
		infile = args.infile

	if args.f:
		redcapForm = args.f

	if infile:
		process()
