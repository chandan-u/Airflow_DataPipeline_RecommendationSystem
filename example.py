

# pipelines
from etl.pipeline import SimpleMovileLensPipeLine

# initialize spark
from pyspark import SparkContext, SparkConf, SQLContext
conf = SparkConf().setAppName("movielens pipleline").setMaster("local[2]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)



if __name__ == "__main__":


    sources = { "movies": "./data/ml-latest-small/movies.csv", "ratings" : "./data/ml-latest-small/ratings.csv", "tags" : "./data/ml-latest-small/tags.csv"}

    pipeline = SimpleMovileLensPipeLine(sources)
     
    pipeline.data_ingestion(sqlContext)
    pipeline.data_transformation()
