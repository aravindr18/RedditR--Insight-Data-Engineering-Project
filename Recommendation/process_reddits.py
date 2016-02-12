""" This module tries to utilize the ALS algorithm on spark
to generate recommendations """

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.storagelevel import StorageLevel
import sys

keyspace = 'flashback'


def main(argv):
    Conf = (SparkConf().setAppName("recommendation").set("spark.hadoop.validateOutputSpecs", "false"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)


    dirPath = "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+argv[1]+".parquet"
    # read the parquet table and register as a table
    rawDF = sqlContext.read.parquet(dirPath).persist(StorageLevel.MEMORY_AND_DISK_SER)\
                    .registerTempTable("comments")
    

    # for our recommendations we need to group by subreddit and author
    # [deleted] are author tags who have deleted their comments or accounts
    # filtering [deleted] is essential as a lot of users and comments have been deleted :) 
    sr_userCount = sqlContext.sql("SELECT author, subreddit , COUNT(*) as count from comments GROUP BY subreddit,author having author!='[deleted]' ")
    
    # Spits out Row(author,subreddit, count ) and dumps it as a json in HDFS
    sr_userCount.write.parquet("hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/recommend/data/sr_userCount.parquet")


    sc.stop()

if __name__ =="__main__":
    main(sys.argv)
