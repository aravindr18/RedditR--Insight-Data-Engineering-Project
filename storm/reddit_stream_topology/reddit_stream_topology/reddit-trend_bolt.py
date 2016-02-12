from pyleus.storm import SimpleBolt
import datetime
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, PreparedStatement
from operator import itemgetter#, attrgetter
import sys
import logging
import logging.config

cluster = Cluster(['172.31.1.44','172.31.1.45','172.31.1.46']) 
session = cluster.connect('flashback')

# top trending subreddit per minute
cql_query_subreddit = "INSERT INTO minute_top_trends (minuteslot,subreddit,count) VALUES (?,?,?)"
cql_trend_stmt = session.prepare(cql_query_subreddit)

# top trending authors per minute
cql_query_author = "INSERT INTO minute_top_authors (minuteslot,author,count) VALUES (?,?,?)"
cql_author_stmt = session.prepare(cql_query_author)


log = logging.getLogger("reddit_stream_topology.minute-bolt")

toLog = False




def execBatch(batch):
	try:
		session.execute(batch)
	except:
		e = sys.exc_info()[0]
		log.exception("Exception on batch insert: " + str(e))

def insert_trends(minuteslot,records):
	
	sorted_r = sorted(records.items(),key=itemgetter(1),reverse=True)[:20]
	
	count=0
	batch = BatchStatement()
	for subreddit,count in sorted_r:
		batch.add(cql_trend_stmt,(minuteslot,subreddit,count))
		count+=1
		if(count==50):
			execBatch(batch)
			count=0
			batch=BatchStatement()

	if (count>0):
		execBatch(batch)

def insert_author(minuteslot,records):
	sorted_r = sorted(records.items(),key=itemgetter(1),reverse=True)[:20]
	count=0
	batch = BatchStatement()
	for author,count in sorted_r:
		batch.add(cql_author_stmt,(minuteslot,author,count))
		count+=1
		if(count==50):
			execBatch(batch)
			count=0
			batch=BatchStatement()

	if (count>0):
		execBatch(batch)

class MinuteBolt(SimpleBolt):
	def initialize(self):
		self.trends={}
		self.author_trends={}
		minuteslot=long(time.strftime('%Y%m%d%H%M'))

	def process_tuple(self,tup):
		author, subreddit, body = tup.values
		if subreddit is None or author is None:
			return
		Key = subreddit
		Author = author
		
		if Key in self.trends:
			self.trends[Key] += 1
		else:
			self.trends[Key] = 1

		if Author in self.author_trends:
			self.author_trends[Author]+=1
		else:
			self.author_trends[Author]=1

	def process_tick(self):
		if toLog:
			log.info("Processing a tick: sizes=%d", len(self.trends))

		self.minuteslot = long(time.strftime('%Y%m%d%H%M'))
		insert_trends(self.minuteslot, self.trends)
		insert_author(self.minuteslot,self.author_trends)
		
		self.trends.clear()
		self.author_trends.clear()
		if toLog:
			log.info("Cleared state: sizes=%d", len(self.trends))

	
if __name__ == '__main__':
	MinuteBolt().run()
