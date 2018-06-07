from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import argparse

infile = ""

def process():
	# read csv
	df = pd.read_csv(infile)

	# drop the full row duplicates
	ndf = df.drop_duplicates()  #keep='last')

	# get those IDs that are dup
	#ndf.loc[ndf.duplicated(["ID"]), :]
	#print(ndf)

	# create a temp dup matrix where the 'is_dup_id' is True
	#dup_ids = ndf[ndf.is_dup_id]
	dups = ndf.loc[ndf.duplicated(["ID"]), :]

	# the dup ids
	di = dups["ID"].drop_duplicates()
	print("Found the following duplicate ids...")
	#print(di)

	# get column names
	colhdrs = ndf.columns
	#print(type(colhdrs))

	colmaxvalues = {}
	allDupGrps = pd.DataFrame(columns=colhdrs)

	for c, dupid in di.iteritems():

		# init a new sub group df to loop through columns getting dups
		dupsubgrp = pd.DataFrame(columns=colhdrs)
		# loop through ndf collecting the row data
		for i, row in ndf.iterrows():
			id = row[0]  # get the id
			#print("Checking: " + str(dupid) + " with " + str(id));
			if dupid == id:  # get the id to compare, if its a dup
				#print("Found the dup " + str(dupid))
				# add values to the df
				dupsubgrp.loc[len(dupsubgrp)] = row.values
				allDupGrps.loc[len(allDupGrps)] = row.values
		#print(dupsubgrp["ID"])
		# now check to see if we have more than one row in our subgrp
		if dupsubgrp["ID"].count() > 1:
			#print("Subgrp total: " + str(dupsubgrp["ID"].count()))
			idx = 0
			# loop trough all the columns to start checking to see how many we need replicate
			for col in colhdrs:
				nonNullTotal = dupsubgrp[col].count()  # get all non null value totals
				nullTotal = sum(pd.isnull(dupsubgrp[col]))  # get those that are null
				rowCount = nonNullTotal + nullTotal
				coldups = dupsubgrp.duplicated([col])  # get the dups within the column
				coldupsvals = coldups.values
				#print(col)
				#print(adups)
				#print(dupsubgrp[col])
				# search of all the  dups (true), gives back non-empty element array
				element = np.where(coldupsvals == True)
				#if cdups.values.max:
				#print(element)
				#print(len(element[0]))
				cnt = len(element[0]) + 1   # add one to include the first dup row
				#a = element[0].tolist()
				#cnt = a.count(True) + 1   # include the first element
					#print("Column has dups")
				if cnt < rowCount:
					v = {'index': str(idx),'repeats': str(rowCount)}
					try:
						maxval = colmaxvalues[col]  #look up the col max val counter
						if rowCount > maxval:
							colmaxvalues[col] = v
					except:
						colmaxvalues[col] = v
						#colmaxvalues[col] = rowCount
				idx = idx + 1
			#print(colmaxvalues)
	# build a new df with expanded columns
	expandedColHdrs = []

	print("Expanding the columns with new variables...")
	for c in colhdrs:
		try:
			repeatMaxVal = colmaxvalues[c]  # check to see if column needs expanded
			idx = 0
			while idx < repeatMaxVal.repeats:
				if idx == 0:
					expandedColHdrs.append(c)
				else:
					nextColName = c + "_" + str(idx)
					expandedColHdrs.append(nextColName)
				idx = idx + 1
		except:
			expandedColHdrs.append(c)

	#print(expandedColHdrs)

	# new df
	expdf = pd.DataFrame(columns=expandedColHdrs)
	#print(expdf)

	# loop throught dup sub grp to merge rows
	#print(allDupGrps["ID"])
	print("Processing the sub-groups and merging data for these dup IDs...")
	grouped = allDupGrps.groupby(allDupGrps["ID"])
	grpids = list(grouped.groups.keys())
	print(grpids)
	print("----------")

	rowDict = []
	# iterate over all the groups
	for grpid in grpids:
		#print(grpid)
		df0 = grouped.get_group(grpid)

		rowCnt = 0
		k = df0.keys()
		listSeries = ""
		# iterate over one grouping
		for dfRow in df0.iterrows():
			lsRow = list(dfRow)  # convert the tuple to a list
			if  rowCnt == 0:  # for the first record only  jumpstart the record
				listSeries = lsRow[1]  # get the Series
				# insert any repeated fields
				for i in range(len(colhdrs)):
					try:
						repeatVal = colmaxvalues[colhdrs[i]] #look up the col max val counter
						n = int(repeatVal['index'])+1
						reps = int(repeatVal['repeats'])
						# add new extra fields to the list
						t = 1
						while t < reps:
							vn =  colhdrs[i] + "_" + str(t)
							listSeries[vn] = ""
							t = t + 1
						#print("Data put at " + str(repeatVal['index']) + " - " + colhdrs[i] + "_" + str(rowCnt))
					except KeyError:
						pass
				#print(listSeries)
			else:  # for the remaining rows, fill in correct field slot
				for i in range(len(colhdrs)):
					try:
						repeatVal = colmaxvalues[colhdrs[i]] #look up the col max val counter
						newLabel = colhdrs[i] + "_" + str(rowCnt)
						dataval = lsRow[1][colhdrs[i]]
						listSeries[newLabel] =  dataval
						#print("Data put at " + str(repeatVal['index']) + " - " + newLabel + " = " + str(dataval))
					except KeyError:
						pass
			rowCnt = rowCnt+1
		d = dict(listSeries) # conver only the data to a dictionary
		rowDict.append(d)

	#print(rowDict)
	print("Processing the merged records...")
	expdf = expdf.from_records(rowDict)

	print("Post clean-up of duplicate reocrds...")
	# Post clean-up, by removing all the duplicate ids
	# no go back and drop the non-dup df with id
	for c, dupid in di.iteritems():
		ndf = ndf[ndf.ID != int(dupid)]  # drop col with specific ID

	print("Merging dataframes...")
	# merge the dataframes
	merged = pd.concat([ndf, expdf])

	print("Writing merged file...")
	fn = infile.split(".")

	# write to a csv
	merged.to_csv(fn[0] + '-merged.csv')

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("infile", help="Data file to merge")
	args = parser.parse_args()

	if args.infile:
		infile = args.infile

	if infile:
		process()
