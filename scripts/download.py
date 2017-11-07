"""
   This script downloads movieLens Data set from :
   'http://files.grouplens.org/datasets/movielens/'
"""

import os 
import sys


try:
    import urllib.request
except ImportError:

	import logging
	logging.info("Issues with urllib import, make sure you are using python3")
    


# dataset download url
#complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
small_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'

	
try:
    
    # saving files @
    datasets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')

    print("creating data set at ", datasets_path)
    if not os.path.isdir(datasets_path):
        os.makedirs(datasets_path)
 
    # saving files as
    small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')

    # download files
    small_f = urllib.request.urlretrieve(small_dataset_url, small_dataset_path)

except:

    import traceback
    import logging
    logging.warn("Error with Download")
    logging.info("Error With Download")
    traceback.print_exc(file=sys.stdout)



