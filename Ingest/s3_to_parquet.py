"""The script pulls file from Amazon S3 bucket and converts them into Parquet columnar storage
on HDFS. Parquet helps us store the text data as a columnar store along with the schema information 
on top of HDFS

The ingest.sh script launches the following .py script
"""
import sys
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel



# Hacky way of converting the datatype of a column from a dataframe
def convertColumn(df, name, new_type):
    df_1 = df.withColumnRenamed(name, "swap")
    return df_1.withColumn(name, df_1["swap"].cast(new_type)).drop("swap")



def main(argv):

  print "started" + argv[1]
  Conf = (SparkConf().setAppName("s3ToParquet"))
  sc = SparkContext(conf=Conf)
  sqlContext = SQLContext(sc)

  # schema information of JSON along with their data-type
  fields = [StructField("archived", BooleanType(), True),  
          StructField("author", StringType(), True),
          StructField("author_flair_css_class", StringType(), True),
          StructField("body", StringType(), True),
          StructField("controversiality", LongType(), True),
          StructField("created_utc", StringType(), True),
          StructField("distinguished", StringType(), True),
          StructField("downs", LongType(), True),
          StructField("edited", StringType(), True),
          StructField("gilded", LongType(), True),
          StructField("id", StringType(), True),
          StructField("link_id", StringType(), True),
          StructField("name", StringType(), True),
          StructField("parent_id", StringType(), True),
          StructField("retrieved_on", LongType(), True),
          StructField("score", LongType(), True),
          StructField("score_hidden", BooleanType(), True),
          StructField("subreddit", StringType(), True),
          StructField("subreddit_id", StringType(), True),
          StructField("ups", LongType(), True)]

  dirPath = "s3n://reddit-comments/" + argv[1]
  # Create a Dataframe using cqlContext
  DF = sqlContext.read.json(dirPath,StructType(fields)).persist(StorageLevel.MEMORY_AND_DISK_SER)
  # convert datatype of created_utc from string to long
  raw_DF = convertColumn(DF,"created_utc","long")

  fileName = argv[1][argv[1].rfind("/")+1:]
  # dump it as a parquet table on HDFS
  raw_DF.write.parquet("hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+fileName+".parquet")

  print "DONE " + argv[1]
  sc.stop()



if __name__ =="__main__":
  main(sys.argv)