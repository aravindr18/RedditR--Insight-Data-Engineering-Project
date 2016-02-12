# This is the reddit producer that produces the message
# 
import time
import reddit_stream
from kafka import SimpleProducer, KafkaClient
import json
import sys
if len(sys.argv) > 1:
    topic = sys.argv[1]
else:
    print("need a kafka topic as an argument")
    exit()

print("using topic:" + topic)

# To send messages synchronously
kafka = KafkaClient('localhost:9092')
#partitionIds = kafka.get_partition_ids_for_topic(topic)
producer = SimpleProducer(kafka)

class SimpleListener(reddit_stream.StreamListener):
    def processMessage(self, message):
        if message['type'] == 'comments':
            item = {}
            item_enc=''
            for comment in message['comments']:
                item['author'] = comment['author']
                item['parent_id'] = comment['parent_id']
                item['score']= comment['score']
                item['name'] = comment['name']
                item['subreddit']= comment['subreddit']
                item['link_url']= comment['link_url']
                item['created_utc']= comment['created_utc']
                item['body'] = comment['body']
                try:

                    item_enc = json.dumps(item) # producer expects the message to be in b type
                except: # sometimes we get an encoding exception 
                    # e.g. "UnicodeDecodeError: 'ascii' codec can't decode byte 0xe2"
                    print("encoding exception...")
                    continue # don't process this tweet if there were issues with encoding 

                producer.send_messages(topic, item_enc) 

userAgent = 'Reddit Stream by /u/datamonkey92'
commentStream = reddit_stream.CommentStream(userAgent, waitSeconds=2.5)

listener = SimpleListener()
listener.start() # This is a Thread, so you have to start it

commentStream.addListener(listener)

commentStream.start() # Also a thread.

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print 'Exiting...'
    listener.stop() # The thread will exit as soon as possible.
    commentStream.stop() # The thread will exit as soon as possible.
