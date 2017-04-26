from elasticsearch import Elasticsearch
import json
import sys
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import boto3

# Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = '826540436-zAte0YI7lGjAQtxWjIqteuwzAJrGl54u9T7xIue1'
ACCESS_SECRET = '3p5OhxIW77Zvp7TMB85A3YoSToRGezESTo6jpTgqM6n4a'
CONSUMER_KEY = 'RCLlUnG5j0fMkCAaFnW3K0x7K'
CONSUMER_SECRET = '42aN2HTKBfHV9K22FP2Pu7wrsvtjaB7ZSI0beMkEWGbv0U9wGX'

sqs = boto3.resource('sqs')
queue = sqs.create_queue(QueueName = 'tweet_sentiment')

index_name = "tweet-idx"

host = "https://search-twitter-sentiment-fdwtpqksdm3j6zvpp6bl2b5eru.us-west-2.es.amazonaws.com"
#Creating Template

mapping = {"mappings": {
        "tweet": {
            "properties": {
                "text": {
                    "type": "string"
                },
                "coordinates": {
                    "type": "geo_point"
                },
                "sentiment": {
                	"type": "string"
                },
                "score":{
                	"type": "double"
                }
            }
        }
}
}

es = Elasticsearch(host)
es.indices.create(index = index_name, body=mapping, ignore=400)

class StdOutListener(StreamListener):
	def on_data(self, data):
		global queue
		try:
			new_data = json.loads(data)
			tweet_text = new_data['text']
			tweet_id = new_data['id']
			Long = None
			Lat = None
			if new_data['coordinates']:
				Long = float(new_data['coordinates']['coordinates'][0])
				Lat = float(new_data['coordinates']['coordinates'][1])
			elif 'place' in new_data.keys() and new_data['place']:
				Long = float(new_data['place']['bounding_box']['coordinates'][0][0][0])
				Lat = float(new_data['place']['bounding_box']['coordinates'][0][0][1])
			elif 'retweeted_status' in new_data.keys() and 'place' in new_data['retweeted_status'].keys() and new_data['retweeted_status']['place']:
				Long = float(new_data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][0])
				Lat = float(new_data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][1])
			elif 'quoted_status' in new_data.keys() and 'place' in new_data['quoted_status'].keys() and new_data['quoted_status']['place']:
				Long = float(new_data['quoted_status']['place']['bounding_box']['coordinates'][0][0][0])
				Lat = float(new_data['quoted_status']['place']['bounding_box']['coordinates'][0][0][1])
			if Lat and Long:
				twitter_data = {
					'Id': {'DataType': 'Number', 'StringValue': str(tweet_id)},
					'Tweet': {'DataType': 'String','StringValue': str(tweet_text)},
					'Latitude': {'DataType': 'Number', 'StringValue': str(Lat)},
					'Longitude': {'DataType': 'Number', 'StringValue': str(Long)}	
				}
				print(twitter_data)
				queue.send_message(MessageBody = "Tweets", MessageAttributes= twitter_data)
		except Exception as e:
			print("ERROR: " + str(e))
		except requests.exceptions.ReadTimeout:
			pass
	def on_error(self, status):
		return(status)

if __name__=='__main__':
	t = StdOutListener()
	auth = OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
	auth.set_access_token(ACCESS_TOKEN,ACCESS_SECRET)
	stream = Stream(auth, t)
	stream.filter(languages = ['en'], track = ['Afganistan', 'trump','googlecloud','Cloudera', 'MOAB','football','elections','food','music'])

                              

