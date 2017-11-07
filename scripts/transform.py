"""

   Script for transforming extracted data and saving it a target location 
   using spark

"""


import os

# pipelines
from etl.pipeline import SimpleMovileLensPipeLine

# initialize spark
try:
    from pyspark import SparkContext, SparkConf, SQLContext
    conf = SparkConf().setAppName("movielens transform pipleline").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
except:

    import logging
    import sys
    import traceback
    logging.error("Error loading spark context")
    logging.warn("make sure env var PYSPARK_PYTHON=python3 and you are using python3 version < 3.6")
    traceback.print_exc(file=sys.stdout)
    sys.exit(1)



if __name__ == "__main__":


    datasets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')

    sources = { "movies": os.path.join(datasets_path, "movies.csv"), "ratings" : os.path.join(datasets_path, "ratings.csv"), "tags" : os.path.join(datasets_path, "tags.csv") }
    targetDir = datasets_path


    pipeline = SimpleMovileLensPipeLine(targetDir, sources)
    pipeline.data_init(sqlContext)
    pipeline.data_transform()



