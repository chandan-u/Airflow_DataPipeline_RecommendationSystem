
"""
  This is the Pipleline module 
"""



class BasePipleline:


    """
      Base class for ETL piplelines. Should be inherited for customization
    """


    def data_init(self):
        pass

    def data_transform(self):
        pass

    def data_load(self):
        pass


class Source():

    location = None
    file_type = None

    
class SimpleMovileLensPipeLine(BasePipleline):


    """
        Source: movielens dataset
        Transformation: 
        Target: csv file with required data 
        
    """

    def __init__(self, targetDir, kwsources):

        self.sources = kwsources.copy()  # sources will be full path of the source data file
        self.targetDir  = targetDir           # targetDir will be the dir locatin to save transformed files
 
       

    def data_init(self, sqlContext):
        
        """
            ingest data from various sources
            in this case data is present on the filesystem itself
            ingest: movies.csv, ratings.csv, tags.csv
        """
   
        # create dataframes
        # expects a header in the file itself
        self.ratings_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(self.sources["ratings"])
        self.tags_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(self.sources["tags"])
        self.movies_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(self.sources["movies"])
   

    def data_transform(self):

        """
            join movieLens movie, ratingsn and tag dataset
            Create a one big fact table with all dimensions
        """ 
       


        # perform full outer join on three tables
        tags_df_renamed = self.tags_df.withColumnRenamed("timestamp", "tag_timestamp")

        # sample tables for logs
        self.ratings_df.show(3)
        tags_df_renamed.show(3)
        self.movies_df.show(3)

        # inner join 
        movielens_fact_df = self.ratings_df.join(tags_df_renamed, ["userId", "movieId"]).join(self.movies_df, "movieId")
        
        # save dataframe to csv
        self.data_load(movielens_fact_df, name='fact_movileLens.csv')


    def data_load(self, df, name):
        
        import os
        df.write.format("com.databricks.spark.csv").save(os.path.join(self.targetDir,name))


        


