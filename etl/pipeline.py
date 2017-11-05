
"""
  This is the Pipleline module 
"""




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

    def __init__(self, **kwsources, **kwtargets):

        self.sources = kwsources.copy()
        self.targets  = kwtargets.copy()

        # initialize spark
        from pyspark import SparkContext, SparkConf
        conf = SparkConf().setAppName(appName).setMaster(master)
        sc = SparkContext(conf=conf)


    def data_ingestion(self):
        
        """
            ingest data from various sources
            in this case data is present on the filesystem itself
            ingest: movies.csv, ratings.csv, tags.csv


        """
    
        
        
         
