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


def insert_cql(cql_prep_stmt, params): 
    try:
        session.execute_async(cql_prep_stmt, params)
    except:
        print "ERROR"

def insert_text(reddit):
    secslot = long(time.time())
    insert_cql(cql_reddit_stmt, [secslot, reddit['subreddit'], reddit['author'], reddit['created_utc'],reddit['body']])
def extract_json(json_line):

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
        insert_text(reddit)
        self.emit((reddit['author'], reddit['subreddit'], reddit['body']), anchors=[tup])

if __name__ == '__main__':
    RedditIngestBolt().run()

