# This python script reads data from S3 and tries to display the first post of a user

# The result will be stored in Cassandra with the following Schema
# User | DateTime | Text | Link Topic | Subreddit
# Also display the timedifference between query time and DateTime returned by cassandra


import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import pyspark_cassandra
from pyspark.storagelevel import StorageLevel
import sys

keyspace = 'flashback'


def main(argv):
    Conf = (SparkConf().setAppName("Flashback"))
    sc = SparkContext(conf=Conf)
    sqlContext = SQLContext(sc)


    dirPath = "hdfs://ec2-52-71-113-80.compute-1.amazonaws.com:9000/reddit/data/"+argv[1]+".parquet"

    rawDF = sqlContext.read.parquet(dirPath).persist(StorageLevel.MEMORY_AND_DISK_SER)\
                    .registerTempTable("comments")
    

    # TimeMachine
    # First post information of every user
    flashback = sqlContext.sql("SELECT author, created_utc, body, link_id, subreddit from comments ")
    flashback.write.format("org.apache.spark.sql.cassandra").options(table ="firstpost", keyspace =  keyspace).save(mode="append")

    print "FLASHBACK DONE"

    # R- Insights
    # GroupBy Subreddits to show some interesting Insights
    subreddit_infos = sqlContext.sql(""" SELECT YEAR(FROM_UNIXTIME(created_utc)) as year, 
                                         MONTH(FROM_UNIXTIME(created_utc)) as month, subreddit, 
                                         COUNT(*) total_comments, 
                                         count(DISTINCT author) distinct_authors, 
                                         count(DISTINCT link_id)     distinct_submissions, 
                                         (COUNT(*) / count(DISTINCT author)) comments_per_distinct_author, 
                                         (COUNT(*)/ count(DISTINCT link_id))     comments_per_distinct_submission
                                         FROM comments GROUP BY YEAR(FROM_UNIXTIME(created_utc)),
                                         MONTH(FROM_UNIXTIME(created_utc)), subreddit 
                                         ORDER BY total_comments DESC LIMIT 100
                                      """)


    subreddit_infos.write.format("org.apache.spark.sql.cassandra").options(table ="subredditinfo", keyspace =  keyspace).save(mode="append")
    print "SUBREDDIT INFO DONE"
    sc.stop()
if __name__ =="__main__":
    main(sys.argv)
