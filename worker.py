import json
import boto3
import multiprocessing
import time
from elasticsearch import Elasticsearch

from watson_developer_cloud import NaturalLanguageUnderstandingV1
import watson_developer_cloud.natural_language_understanding.features.v1 as features


## establish connection with watson API
nlu = NaturalLanguageUnderstandingV1(
    version='2012-02-27',
    username='5e95d8c8-cebe-4ed6-bc9b-6e6f14c4dbab',
    password='pF2RFmRaq6tP')


# ##function to get
# response = nlu.analyze(
#     text='The quick brown fox jumped over the lazy dog',
#     features = [features.Sentiment()]
# )
#
# s = json.dumps(response)
# r = json.loads(s)
# print(r['sentiment']['document']['label'])
#print(json.dumps(response, indent=2))

host = ["search-twitter-map-h2myw2tj53kyobspflwj2qpuem.us-east-1.es.amazonaws.com"]

index_name = "tweet-idx"
sqs= boto3.resource('sqs')      #sqs instance
sns = boto3.client('sns')     #sns instance

###SNS####
arn = "arn:aws:sns:us-east-1:040667233965:tweet-map"

queue = sqs.get_queue_by_name(QueueName = 'tweet')
#sentiment = ["positive","negative","neutral"]
es = Elasticsearch(host)
if es:
    print("elastic search host is connected...")

#es.indices.create(index = "tweet_s", body=mapping, ignore=400)
#mapping = {"mappings": {
#        "tweet": {
#            "properties": {
#                "text": {
#                    "type": "string"
#                },
#                "coordinates": {
#                    "type": "geo_point"
#                },
#                "sentiment": {
#                	"type": "string"		#why is the sentiment type string? it should be numerical??
#                }
#            }
#        }
#}
#}

def worker_task():
    while True:
        msgs = queue.receive_messages(MessageAttributeNames=['Id','Tweet','Latitude','Longitude'])
        if len(msgs)>0:
            for message in msgs:
                id = message.message_attributes.get('Id').get('StringValue')
                tweet = message.message_attributes.get('Tweet').get('StringValue')
                lat = message.message_attributes.get('Latitude').get('StringValue')
                lon = message.message_attributes.get('Longitude').get('StringValue')

                try:
                    response = nlu.analyze(text = tweet,features= [features.Sentiment()])
                    s = json.dumps(response)
                    r = json.loads(s)
                    sentiment = r['sentiment']['document']['label']
                    senti_score = r['sentiment']['document']['score']
                    sns_message = {'id':id, 'tweet':tweet, 'lat': lat, 'lon': lon, 'senti': sentiment, 'score': senti_score }
                    print("SNS message:"+ str(sns_message))
                    sns.publish(TargetArn = arn, Message=json.dumps({'default':json.dumps(sns_message)}))
                except Exception as e:
                    print("ERROR:"+ str(e))

        else:
            time.sleep(1)

pool = multiprocessing.Pool(10,worker_task(), (queue,))

worker_task()
while True:
    pass