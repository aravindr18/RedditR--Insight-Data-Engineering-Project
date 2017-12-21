# RedditR- Live Trends and Engagement on Reddit
> This is the project I carried out during the seven-week <a href="http://insightdataengineering.com/">Insight Data Engineering Fellows Program </a>
which helps recent grads and experienced software engineers learn the latest open source technologies 
by building a data platform to handle large, real-time datasets. <br/>
RedditR is a real time content engagement platform to track trends as they happen on reddit.
~~You can find the app at <a href="http://www.redditr.space"> www.redditr.space</a>~~

## Motivation
The <b>Functional </b>motivation for this project was to:
1. Create real-time trends on reddit
2. Experimental Subreddit Recommendation System at scale
3. Finding User posts (and demonstrating the power of no-sql database in finding needle in a haystack and analytics)

The <b>Engineering</b> movitation behind building such a product is to demonstrate the ability to create a scalable real-time big data
pipleline using open source technologies. 

## Data Pipeline
![alt tag](https://raw.githubusercontent.com/aravindk1992/RedditR--Insight-Data-Engineering-Project/master/insightpipeline.png)
Thanks to /r/Stuck_In_the_Matrix for providing the historical dataset from October 2007- December 2015 for this project.  The entire repo is <a href="http://couch.whatbox.ca:36975/reddit/submissions/monthly/"> here</a>. The data is a monthly dump that is in the bz2 file format. The file dump was then downloaded to s3 bucket on Amazon AWS and uncompressed using a `wget` request in python. ( This was automated using a bash script) 
In Order to ensure fast processing on <b>Spark</b>, the file from s3 was processed and compressed into <b>Parquet</b>. Parquet is a columnar store that helps us store the data(JSONs) with its schema on HDFS there by leveraging the fault tolerance and distributed nature of the Hadoop Distributed File System. The original dataset, roughly about <b>1084.5 GB </b> when compressed into Parquet was a mere <b>187.8 GB </b> which gave us a lot of space savings with queries running <b>3x faster</b> on Spark as compared to text data.  Checkout the <b> Ingest</b> folder for implementation details. 

<b> Spark </b> is used for Batch Processing. Check out the <b> Batch </b> folder for implementation and code in python. The feature implemented is called "Flashback" that lets user input their username and the system shows the first ever post of that particular user. There is also support to download all the posts of a user as a json file which may then be used to carry out user profiling. 
The next feature implemeted in pyspark was the recommendation and user interaction which can be found in the <b>SimpleGraph</b> folder. The idea is to connect users who have interacted with each other by a directed graph and grouping them by the subreddits. Once this is done, we then compute the indegree and outdegree of everynode in the clusters of subreddits. This gives us an idea as so to who the most influential and active users in a particular subreddits are. This helps us maximize content engagement as it would make more sense to interact often with these influential users to elicit maximum viewability and interaction on your posts. The recommendation is implemented by looking up at the subreddits that the influential users of a subreddit of your liking are active on and then suggesting these subreddits as recommendations. 
<b> Recommendation </b> folder contains implementation of a collaborative filter algorithm (ALS) in python. This was done to validate the user graph approach that I came up with. The results were very intuitive and promising. The user graph model also scored much higher in terms of compute time as it took <b> 4.6 mins </b> for computing recommendations as compared to the ALS approach with 20 iterations which took approximately <b> 13 hours </p>
<br/>
Real time processing was done using Storm and Kafka was used as the publish-subscribe broker. Implementation can be found in the <b> storm</b> folder. 
<br/> real-time reddit api can be found in the <b> Stream</b> folder
<b> Cassandra </b> was chosen as the key-value store for this project.

