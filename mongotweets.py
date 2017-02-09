import json
import time
import os
import io
import logging
import sys

from pymongo import MongoClient
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CONSUMER_KEY = 'wm2KsU371fxKAnsyVpuSgHkk8'
CONSUMER_SECRET = '7qYr9BtdW6EiJDqykuWhomOQMvuWxCe1nITDxCCybHek5OIlIX'
ACCESS_TOKEN = '266469690-igoeXixjISwzJJhcU1GLwQoDYKJxj7uJbF9h4cvq'
ACCESS_SECRET = 'AHeSWTjWEYkmPIKyUhRrGvWaf9Y85pmuJzsnQVTaWfvrG'


class listener(StreamListener):
	def __init__(self, start_time, time_limit=60):
		self.time = start_time
		self.limit = time_limit

	def on_data(self, data):
		while (time.time() - self.time) < self.limit:
			try:
				client = MongoClient('localhost', 27017)
				db = client['twitter_db']
				collection = db['twitter_collection']
				tweet = json.loads(data)
				collection.insert(tweet)
				return True
			except BaseException, e:
				print 'failed ondata,', str(e)
				time.sleep(5)
				pass
		exit()

	def on_error(self, status):
		print status


def get_authentication(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET):
	auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
	return auth


def collect_tweets(auth, start_time, keyword_list, lang_list):
	twitterStream = Stream(auth, listener(start_time, time_limit=20))
	twitterStream.filter(track=keyword_list, languages=lang_list)


if __name__ == '__main__':
	start_time = time.time()
	keyword_list = ['#apple']
	lang_list = ['en']
	auth = get_authentication(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
	logger.info('Authentification completed')
	logger.info('Collecting tweets in %s containing keywords %s...', lang_list, keyword_list)
	collect_tweets(auth, start_time, keyword_list, lang_list)
