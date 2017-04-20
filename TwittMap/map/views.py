from django.shortcuts import render
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import urllib2
from .models import NewTweets
import json
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import certifi

#from textblob import TextBlob

index_name = "twitter-idx"
host = "search-twitter-map-h2myw2tj53kyobspflwj2qpuem.us-east-1.es.amazonaws.com"
host_bhagya = "search-new-twitter-i2pdnr6a2chkajagb2cm5ggd2y.us-west-2.es.amazonaws.com"

host_demo = "search-twitter-sentiment-fdwtpqksdm3j6zvpp6bl2b5eru.us-west-2.es.amazonaws.com"

def pull_stream(request):
    es = Elasticsearch("https://"+host_demo)
    #print("connect to host")
    if request.method == "POST":
        term = request.POST['keyword']
        #print(term)
        total = es.search(index="final-data", body= {"from":0, "size": 5000, "query": {"match": {"tweet": term}}})
        #print(total)
        result = []
        for rec in total['hits']['hits']:
            #print(rec['_source']['tweet'])
            #print(rec['_source']['location'])
            result.append([rec['_source']['tweet'], rec['_source']['location']])
        response ={"tweet_coordinates":result, "num_records":len(result)}
        #print(response)
        #return render(response, 'map/header.html')
        # return(term)
        return HttpResponse(json.dumps(response), content_type="application/json",status=200)
    else:
        return render(request, 'map/header.html')

def tweets_location(request):
    es = Elasticsearch("https://"+host_demo)
    #print("connect to host")
    response ={}
    if request.method == "POST":
        term = request.POST['keyword']
        distance = float(request.POST['distance'])
        Lat = float(request.POST['lat'])
        Long = float(request.POST['lon'])
        location_tweets = es.search(index = index_name, body = {"from":0, "size": 1000, "query": {"match": {"tweet": term}},"filter": {"geo_distance":{"distance": str(distance)+"km","location":{"lat": Lat,"lon":Long}}}})
        result = []
        for rec in location_tweets['hits']['hits']:
            result.append([rec['_source']['tweet'], rec['_source']['location']])
        response ={"tweet_coordinates":result, "num_records":len(result)}
        #print(response)
    return HttpResponse(json.dumps(response), content_type="application/json",status=200)



def index(request):
    #GEOBOX_WORLD = [-180,-90,180,90]
    #streamer = TweetStreamListener()
    #stream = Stream(auth,streamer, timeout=500)
    #stream.filter(locations=GEOBOX_WORLD, languages=['en'], track=['springbreak','trump','WWE','food','#WomensHistoryMonth','UPElectiond2017',''])
    #upload_tweets(host_demo,"tweetstream.json",index_name)
    data = pull_stream(request)
    for i in data:
        print(i)
    return render(request, 'map/header.html')

@csrf_exempt
def sns_test_handler(request):
	if request.method == "POST":
		header = json.loads(request.body)
		print("Post Request")
		if 'Type' in header.keys():
			if header['Type']=="SubscriptionConfirmation":
				print("Request Received")
				urlSub = headers['SubscribeURL']
				data_response = urllib2.urlopen(urlSub).read()
				print("Subscription successful")
			elif header['Type']=="Notification":
				print("Received new message" + str(header['Message']))
				new_message = json.loads(json.loads(header['Message']).get('default'))
				print("New message is:" + str(new_message))
				tweet_id = message.get('id')
				tweet_text = message.get('tweet')
				tweet_lat = message.get('lat')
				tweet_long = message.get('lon')
				tweet_sentiment = message.get('senti')
				tweet_score = message.get('score')
				es = Elasticsearch("https://" + host_demo)
				es.index(index= name, id=id, doc_type="tweet", body={"tweet": tweet, "location": {"lat": lat, "lon": lon}, "sentiment":sentiment, "score": score})
				new_Tweet = NewTweets(id=tweet_id, tweet=tweet_text, lat= tweet_lat, lon= tweet_lon, sentiment=tweet_sentiment, score=tweet_score)
				new_Tweet.save()
		return render(request, 'map/header.html', {"params": str(request.POST)})		