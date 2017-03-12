from django.shortcuts import render
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import certifi


#twitter api credentials:
Access_token = "1656072950-RJFyIBI8JQRKH8DwrdB66TfjibaRV0uds3X62kP"
Access_secret = "scPMTHO0YMFcxTDuvgJYERC5SmDLJuSfjlnxbKVsiv5TN"
Consumer_key = "1lJ9ioOr10ZoFzu8iGhFqDIQb"
Consumer_secret = "PFwG0WSpl5V4EiwwxbLoVMIKcq40pujLIvDGaMd5Cva32opLwf"

auth = OAuthHandler(Consumer_key, Consumer_secret)
auth.set_access_token(Access_token, Access_secret)

class TweetStreamListener(StreamListener):
    def __init__(self, time_limit=500):
        self.start_time = time.time()
        self.limit = time_limit
        #self.saveFile = file
        super(TweetStreamListener, self).__init__()
    def on_data(self, data):
        if(time.time() - self.start_time) < self.limit:
            json_data = json.loads(data)
            #print(json_data)
            with open('map/tweetstream.json','w') as pen:
                json.dump(json_data,pen)
            return True
        else:
            return False
    def on_error(self, status):
        if status.code == 420:
            return False


mapping = {"mappings": {
            "tweet": {
                "properties": {
                    "text": {
                        "type": "string"
                    },
                    "coordinates": {
                        "properties": {
                            "type": "geo_point"
                        }
                     }
                }
            }
    }
    }

def upload_tweets(host, file, idx):
    if host:
        es = Elasticsearch("https://"+host)
        if(es):
            print("connected...")
    else:
        es = Elasticsearch()    #local by default
    es.indices.create(index=idx, body=mapping, ignore=400)
    f = open("map/tweetstream.json", "r")
    for line in f:
            print(line)
            #if line.strip()=="":
             #   continue
            try:
                data = json.loads(line)
                tweet_text = data["text"]
                tweet_id = data["id"]
                print(tweet_text)
                Long =None
                Lat = None
                if data['coordinates']:
                    Long = float(data['coordinates']['coordinates'][0])
                    Lat = float(data['coordinates']['coordinates'][1])
                elif 'place' in data.keys() and data['place']:
                    Long = float(data['place']['bounding_box']['coordinates'][0][0][0])
                    Lat = float(data['place']['bounding_box']['coordinates'][0][0][1])
                elif 'retweeted_status' in data.keys() and 'place' in data['retweeted_status'].keys() and \
                    data['retweeted_status']['place']:
                    Long = float(data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][0])
                    Lat = float(data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][1])
                elif 'quoted_status' in data.keys() and 'place' in data['quoted_status'].keys() and \
                    data['quoted_status']['place']:
                    lon = float(data['quoted_status']['place']['bounding_box']['coordinates'][0][0][0])
                    lat = float(data['quoted_status']['place']['bounding_box']['coordinates'][0][0][1])
                if Lat and Long:
                    print(Lat,",",Long)
                    es.index(index=idx, id=tweet_id, doc_type="tweet",body={"tweet": tweet_text, "location": {"lat": Lat, "lon": Long}})
                    print("check....")
            except Exception as e:
                print("ERROR: " + str(e))
    f.close()

