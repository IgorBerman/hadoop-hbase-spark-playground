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

class TweetsListener(StreamListener):
	def __init__(self, tablename, conn):
		self._conn = conn
		self._table = conn.table(tablename)
	
	def on_data(self, data):
		tweet = json.loads(data)
		if 'id' in tweet and 'text' in tweet and 'timestamp_ms' in tweet and 'user' in tweet:
			print "\n\n\n-----------------------\n",tweet['id']," ",tweet['text']," ",tweet['timestamp_ms']," ",tweet['user']['id']
			hashtags = ",".join(map(lambda hashtag: hashtag['text'].encode('ascii','ignore') if hashtag['text'] else '', tweet['entities']['hashtags']))
			text = tweet['text'].encode('ascii', 'ignore') if tweet['text'] else ''
			place = tweet['place']
			country = place['country'].encode('ascii', 'ignore') if (place and 'country' in place) else ''
			self._table.put(tweet['id_str'], {'general:text': text, 'general:timestamp':tweet['timestamp_ms'],\
				'general:user_id':tweet['user']['id_str'], 'general:lang':tweet['lang'], 'general:country':country,\
				'hashtags:':hashtags})
		#e.g. delete messages
		#else:
		#	print "\n\n\n---------unexpected tweet format -------\n", tweet
		return True

	def on_error(self, status):
		print status

def create_tweets_table(tablename, conn):
	print 'checking if %s table exists' % tablename
	if tablename not in conn.tables():
		families = {
			'general': dict(),  # use defaults
			'hashtags':dict()
		}
		print 'creating %s table' % tablename
		conn.create_table(tablename, families)
		
if __name__ == '__main__':
	'''
	python /vagrant/twitter.py -ck <your-consumer-key> -cs <your-consumer-secret> -at <your-access-token> -ats <your access token secret> -t tweets -f <comma-separated-list-of-words>
	'''
	parser = argparse.ArgumentParser(description='Read tweets by filter.')
	parser.add_argument('-ck', '--conskey', required=True)
	parser.add_argument('-cs', '--conssecret', required=True)
	parser.add_argument('-at', '--accesstoken', required=True)
	parser.add_argument('-ats', '--accesstokensecret', required=True)
	parser.add_argument('-t', '--table', required=True)
	parser.add_argument('-f', '--filter')
	parser.add_argument('-b', '--hbase', default="localhost")
	parser.add_argument('-p', '--hbaseport', type=int, default=9090)
	args = parser.parse_args()
	hbase_conn = happybase.Connection(args.hbase, args.hbaseport)
	create_tweets_table(args.table, hbase_conn)
	l = TweetsListener(args.table, hbase_conn)
	
	auth = OAuthHandler(args.conskey, args.conssecret)
	auth.set_access_token(args.accesstoken, args.accesstokensecret)
	stream = Stream(auth, l)
	languages=["en", "es", "he"]
	if args.filter:
		print "going to track all tweets with %s" % args.filter
		stream.filter(track=args.filter.split(","),languages=languages)
	else:
		print "going to sample random tweets"
		stream.sample(languages=languages)
