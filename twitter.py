#Import the necessary methods from tweepy library
import sys
import argparse
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import happybase

#http://adilmoujahid.com/posts/2014/07/twitter-analytics/
#http://al333z.github.io/2015/02/28/TheDress/

#This is a basic listener that just prints received tweets to stdout.
class TweetsListener(StreamListener):
	def __init__(self, conn):
		self._conn = conn
		self._table = conn.table('tweets')
	
	def on_data(self, data):
		tweet = json.loads(data)
		print "\n\n\n-----------------------\n",tweet['id']," ",tweet['text']," ",tweet['timestamp_ms']," ",tweet['user']['id']
		print ",".join(map(lambda hashtag: hashtag['text'], tweet['entities']['hashtags']))
		text = tweet['text'].encode('ascii', 'ignore') if tweet['text'] else ''
		self._table.put(tweet['id_str'], {'general:text': text, 'general:timestamp':tweet['timestamp_ms'], 'general:user_id':tweet['user']['id_str']})
		return True

	def on_error(self, status):
		print status

def create_tweets_table(conn):
	print 'checking if tweets table exists'
	if 'tweets' not in conn.tables():
		families = {
			'general': dict(),  # use defaults
			'hashtags':dict()
		}
		print 'creating tweets table'
		conn.create_table('tweets', families)
		
if __name__ == '__main__':
	'''
	python /vagrant/twitter.py -ck <your-consumer-key> -cs <your-consumer-secret> -at <your-access-token> -ats <your access token secret> -f <comma-separated-list-of-words>
	'''
	parser = argparse.ArgumentParser(description='Read tweets by filter.')
	parser.add_argument('-ck', '--conskey', required=True)
	parser.add_argument('-cs', '--conssecret', required=True)
	parser.add_argument('-at', '--accesstoken', required=True)
	parser.add_argument('-ats', '--accesstokensecret', required=True)
	parser.add_argument('-f', '--filter', required=True)
	args = parser.parse_args()
	hbase_conn = happybase.Connection("localhost", 9090)
	create_tweets_table(hbase_conn)
	l = TweetsListener(hbase_conn)
	
	auth = OAuthHandler(args.conskey, args.conssecret)
	auth.set_access_token(args.accesstoken, args.accesstokensecret)
	stream = Stream(auth, l)
	print "going to track all tweets with %s" % args.filter
	stream.filter(track=args.filter.split(","))
