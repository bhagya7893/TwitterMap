try:
    import json
except ImportError:
    import simplejson as json

# Import the necessary methods from "twitter" library
#from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

	#t = MyStreamListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth)
 
print (api.trends_place(1))