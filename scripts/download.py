


import os 

try:
    import urllib.request
except ImportError:

	import logging
	logging.info("Issues with urllib import, make sure you are using python3")
    



#complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
small_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'



try:
    
    datasets_path = os.path.join('..', 'datasets')

    #complete_dataset_path = os.path.join(datasets_path, 'ml-latest.zip')
    small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')

    small_f = urllib.urlretrieve (small_dataset_url, small_dataset_path)
    #complete_f = urllib.urlretrieve (complete_dataset_url, complete_dataset_path)

except:

    import traceback

	logging.info("Error with Download")
    traceback.print_exc(file=sys.stdout)



