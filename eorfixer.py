import os

for f in os.listdir("."):
	if f != str("eorfixer.py"):
		if os.path.isfile(f):
			print(f)
			with open(f, "a") as afile:
				afile.write("E_O_R")
