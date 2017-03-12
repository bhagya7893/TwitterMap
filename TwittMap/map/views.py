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

#from textblob import TextBlob

index_name = "twitter-map"
host = "search-twitter-map-h2myw2tj53kyobspflwj2qpuem.us-east-1.es.amazonaws.com"
host_bhagya = "search-new-twitter-i2pdnr6a2chkajagb2cm5ggd2y.us-west-2.es.amazonaws.com"

def pull_stream(request):
    es = Elasticsearch("https://"+host_bhagya)
    print("connect to host")
    if request.method == "POST":
        term = request.POST['keyword']
        #print(term)
        total = es.search(index="new-twitter-idx", body= {"from":0, "size": 5000, "query": {"match": {"tweet": term}}})
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



def index(request):
    #GEOBOX_WORLD = [-180,-90,180,90]
    #streamer = TweetStreamListener()
    #stream = Stream(auth,streamer, timeout=500)
    #stream.filter(locations=GEOBOX_WORLD, languages=['en'], track=['springbreak','trump','WWE','food','#WomensHistoryMonth','UPElectiond2017',''])
    #upload_tweets(host,"tweetstream.json",index_name)
    #data = pull_stream(request)
    #for i in data:
     #   print(i)
    return render(request, 'map/header.html')
