
"""
  This is the Pipleline module 
"""


# initialize spark
from pyspark import SparkContext, SparkConf, SQLContext
conf = SparkConf().setAppName("movielens pipleline").setMaster("local[2]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


class BasePipleline:


    """
      Base class for ETL piplelines. Should be inherited for customization
    """


    def data_ingestion(self):
        pass

    def data_transformation(self):
        pass

    def data_loading(self):
        pass


    
class SimpleMovileLensPipeLine(BasePipleline):


    """
        Source: movielens dataset
        Transformation: 
        Target: csv file with required data 
        
    """

    def __init__(self, kwsources):

        self.sources = kwsources.copy()
        self.target  = None
       

    def data_ingestion(self):
        
        """
            ingest data from various sources
            in this case data is present on the filesystem itself
            ingest: movies.csv, ratings.csv, tags.csv


        """
    
        ratings_rdd = sc.textFile(self.sources["ratings"])
        tags_rdd = sc.textFile(self.sources["tags"])
        movies_rdd = sc.textFile(self.sources["movies"])



    def data_transformation(self):

        # split each line into cols
        ratings_rdd_t1 = ratings_rdd.map(lambda line: line.split(","))
        tags_rdd_t1 = tags_rdd.map(lambda line: line.split(","))
        movies_rdd_t1 = movies_rdd.map(lambda line: line.split(","))

        # join the rdd's

        ratings.join(tags)

    def data_loading(self, )


if __name__ == "__main__":
    sources = { "movies": "../data/ml-latest-small/ratings.csv", "ratings" : "../data/ml-latest-small/ratings.csv", "tags" : "../data/ml-latest-small/tags.csv"}


    pipeline = SimpleMovileLensPipeLine(sources)
     

