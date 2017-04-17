import sys
import re
import codecs # UniCode support
from pymongo import MongoClient
#from pymongo import Connection # For DB Connection
#from pymongo.errors import ConnectionFailure # For catching exeptions

def main():
  
  # MongoDB connection
  try:
    #db_conn = Connection(host="localhost", port=27017) # Here we specified the default parameters explicitly
    db_conn = MongoClient('mongodb://localhost:27017/')
    print ("Connected to MongoDB successfully!")
  except:
    sys.stderr.write("Could not connect to MongoDB: %s" % e)

  # Grab a database
  db_target = db_conn["mydocs"]
  afile = "JobDescription.txt"
  # Open file
  f = codecs.open(afile, 'r', encoding='utf-8') # Codecs instead regular 'open' to handle UTF-8

  # Read each line
  for line in f:
    
    try: # For exception catching
      wc = len(line.split())

      if wc > 0:  # wordcnt > zero
        # Create dictionary object
        record = {
          "text": line,
          "wordcnt": wc,
          "docref" : afile
        }
      
        # Insert document into DB
        db_target.records.insert(record) # Collections ('records' here) are lazily created
      
    # Exception handler
    except IndexError:
      sys.stderr.write( "Something wrong with this line: " + line + '\n')
      continue
    
  # Close file
  f.close()
  
if __name__ == "__main__":
  main()