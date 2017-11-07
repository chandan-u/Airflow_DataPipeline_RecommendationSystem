

# pipelines
from etl.pipeline import SimpleMovileLensTransformPipe

# initialize spark

try:
    from pyspark import SparkContext, SparkConf, SQLContext
    conf = SparkConf().setAppName("movielens transform pipleline").setMaster("local[2]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
except:

    import logging
    logging.info("Error loading spark context")



if __name__ == "__main__":


    datasets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')
    small_dataset_path = os.path.join(dataset_path, zip_file_name)

    sources = { "movies": os.path.join(datasets_path, "movies.csv"), "ratings" : os.path.join(datasets_path, "ratings.csv"), "tags" : os.path.join(datasets_path, "tags.csv") }
    targetDir = datasets_path


    pipeline = SimpleMovileLensTransformPipe(targetDir, sources)
    pipeline.data_init(sqlContext)
    pipeline.data_transform()



