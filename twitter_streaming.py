# Import the necessary package to process data in JSON format
try:
    import json
except ImportError:
    import simplejson as json

# Import the necessary methods from "twitter" library
#from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

#oauth = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

# Initiate the connection to Twitter Streaming API
#twitter_stream = TwitterStream(auth=oauth)

# Get a sample of the public data following through Twitter
#iterator = twitter_stream.statuses.filter(track="Google", language="en")

class MyStreamListener(StreamListener):
	def on_data(self, data):
		try:
			print(data)
		except requests.exceptions.ReadTimeout:
			pass
		#for i in data:
			#if "\"coordinates\":[" in i:	
		
		
	def on_error(self, status):
		if status_code == 420:
			return False

if __name__=='__main__':
	t = MyStreamListener()
	auth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN,ACCESS_SECRET)
	stream = Stream(auth, t)
	stream.filter(languages = ['en'], track = ['GameofThrones', 'trump','googlecloud','Cloudera', 'WomensHistoryMonth','football','UPElections2017','food','music'])
    
