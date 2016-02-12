""" Real-time processing """

from pyleus.storm import SimpleBolt
import simplejson as json
import datetime
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, PreparedStatement

cluster = Cluster(['172.31.1.44','172.31.1.45','172.31.1.46']) 
session = cluster.connect('flashback')

cql_query = "INSERT INTO rt_reddit (secslot,subreddit,author,created_utc,body) VALUES (?,?,?,?,?)"
cql_reddit_stmt = session.prepare(cql_query)



    
def extract_json(json_line):

    """ simple json is slightly faster to use to load jsons than the default json """
    try:
        item = json.loads(json_line)
    except:
	   return None 

    reddit = {}
    reddit['author'] = item['author']
    reddit['subreddit'] = item['subreddit']
    reddit['body'] = item['body']
    reddit['created_utc'] = item['created_utc']

    return reddit

class RedditIngestBolt(SimpleBolt):

    OUTPUT_FIELDS = ['author', 'subreddit', 'body']
    def process_tuple(self, tup):
        json_reddit, = tup.values
        reddit = extract_json(json_reddit)
        # find the current time
        secslot = long(time.time())
        # execute cassandra insert for real-time comments
        session.execute_async(cql_reddit_stmt, [secslot, reddit['subreddit'], reddit['author'], reddit['created_utc'],reddit['body']])
        # emit the values for the next bolt!
        self.emit((reddit['author'], reddit['subreddit'], reddit['body']), anchors=[tup])

if __name__ == '__main__':
    RedditIngestBolt().run()

