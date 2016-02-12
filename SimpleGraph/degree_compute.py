
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
import sys




def main(argv):
    Conf = (SparkConf().setAppName("SimpleGraph"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)
    keyspace = 'flashback'

    dirPath = "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+argv[1]+"-selfjoin.parquet"

    rawDF = sqlContext.read.parquet(dirPath).persist(StorageLevel.MEMORY_AND_DISK_SER).registerTempTable("self_join")
    

    indegree = sqlContext.sql("Select Subreddit as subreddit, OrigAuth as author, count(*) as rank from self_join group by Subreddit,OrigAuth ")
    
    indegree.write.format("org.apache.spark.sql.cassandra").options(table ="indegree", keyspace =keyspace).save(mode="append")

    outdegree = sqlContext.sql("Select Subreddit as subreddit, RespAuth as author, count(*) as rank from self_join group by Subreddit,RespAuth")
	
    outdegree.write.format("org.apache.spark.sql.cassandra").options(table ="outdegree",keyspace =keyspace).save(mode="append")
    

if __name__ =="__main__":
    main(sys.argv)






