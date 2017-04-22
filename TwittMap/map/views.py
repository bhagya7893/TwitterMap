from django.shortcuts import render
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import urllib.request
from .models import New_Tweets
import json
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import certifi

#from textblob import TextBlob

index_name = "tweet-idx"

host = "search-twitter-map-h2myw2tj53kyobspflwj2qpuem.us-east-1.es.amazonaws.com"

def pull_stream(request):
    es = Elasticsearch("https://"+host)
    #print("connect to host")
    if request.method == "POST":
        term = request.POST['keyword']
        #print(term)
        total = es.search(index=index_name, body= {"from":0, "size": 5000, "query": {"match": {"tweet": term}}})
        #print(total)
        result = []
        for rec in total['hits']['hits']:
            #print(rec['_source']['tweet'])
            #print(rec['_source']['location'])
            result.append([rec['_source']['tweet'], rec['_source']['location']])
        response ={"tweet_coordinates":result, "num_records":len(result)}
        print(response)
        #return render(response, 'map/header.html')
        # return(term)
        return HttpResponse(json.dumps(response), content_type="application/json",status=200)
    else:
        return render(request, 'map/header.html')

def tweets_location(request):
    es = Elasticsearch("https://"+host)
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

    # data = (request)
    # for i in data:
    #     print(i)
    return render(request, 'map/header.html')
    
def poll_data(request):
	while True:
		try:
			data_stream =    New_Tweets.objects.all()
			if len(data_stream)==0:
				time.sleep(0.5)
			else:
				tweets = []
				for ds in data_stream:
					tweets.append({"id": ds.id , "tweet": ds.tweet ,"lat": ds.lat ,"lon": ds.lon,"sentiment": ds.sentiment, "score": ds.score })
					response = {"new_tweets": tweets}
					New_Tweets.objects.all.delete()
					return HttpResponse(json.dumps(response), content_type="application/json", status= 200)
		except:
			return HttpResponse(json.dumps({}),content_type="application/json",status = 200 )

@csrf_exempt
def sns_test_handler(request):
	if request.method == "POST":
		header = json.loads(request.body.decode('utf-8'))
		print("Post Request")
		if 'Type' in header.keys():
			if header['Type']=="SubscriptionConfirmation":
				print("Request Received")
				urlSub = header['SubscribeURL']
				data_response = urllib.request.urlopen(urlSub).read()
				print("Subscription successful")
			elif header['Type']=="Notification":
				print("Received new message" + str(header['Message']))
				new_message = json.loads(json.loads(header['Message']).get('default'))
				print("New message is:" + str(new_message))
				tweet_id = new_message.get('id')
				tweet_text = new_message.get('tweet')
				tweet_lat = new_message.get('lat')
				tweet_long = new_message.get('lon')
				tweet_sentiment = new_message.get('senti')
				tweet_score = new_message.get('score')

				##Elastic search index being filled:
				es = Elasticsearch("https://" + host)
				es.index(index = index_name, id=tweet_id, doc_type="tweet", body={"tweet": tweet_text, "location": {"lat": tweet_lat, "lon": tweet_long}, "sentiment":tweet_sentiment, "score": tweet_score})

				## New_Tweet from Models:
				#new_Tweet = New_Tweets(id=tweet_id, tweet=tweet_text, lat= tweet_lat, lon= tweet_long, sentiment=tweet_sentiment, score=tweet_score)
				#new_Tweet.save()
		return render(request, 'map/header.html', {"params": str(request.POST)})		