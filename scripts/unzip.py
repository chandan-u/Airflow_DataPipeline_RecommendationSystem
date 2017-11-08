"""
   unzip the downloaded zip files

   TODO: Implement this is a function
"""


import os


zip_file_name = 'ml-latest-small.zip'

dataset_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')
small_dataset_path = os.path.join(dataset_path, zip_file_name)

try: 

    import zipfile
    
    with zipfile.ZipFile(small_dataset_path, "r") as z:
        z.extractall(dataset_path)

except:

	import logging 
	import traceback
	import sys

	logging.warn("Issue with unzipping files")
	traceback.print_exc(file=sys.stdout)
    



