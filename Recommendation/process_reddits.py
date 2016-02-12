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

    rawDF = sqlContext.read.parquet(dirPath).persist(StorageLevel.MEMORY_AND_DISK_SER)\
                    .registerTempTable("comments")
    

    # Recommendation System
    sr_userCount = sqlContext.sql("SELECT author, subreddit , COUNT(*) as count from comments GROUP BY subreddit,author having author!='[deleted]' ")
    # Spits out Row(author,subreddit, count ) and dumps it as a json in HDFS
    
    sr_userCount.write.parquet("hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/recommend/data/sr_userCount.parquet")


    sc.stop()




    #subreddit_infos.write.format(("org.apache.spark.sql.cassandra").options(table ="subredditinfo", keyspace =  keyspace).save(mode="append")

if __name__ =="__main__":
    main(sys.argv)
