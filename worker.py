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
    username= "14836d99-7f71-4da6-aaf7-582cd56e6443",   #bhagya
    password= "Yzy346wlSqek"
    #username='5e95d8c8-cebe-4ed6-bc9b-6e6f14c4dbab',   #Suhita
    #password='pF2RFmRaq6tP'
     )


host = "https://search-twitter-sentiment-map-ub6eu35cqjh6kxwgwkzenq4znm.us-east-1.es.amazonaws.com"

index_name = "tweet-idx"
sqs= boto3.resource('sqs')      #sqs instance
sns = boto3.client('sns')     #sns instance

#topic = sns.create_topic(Name = "tweets")

#topic_arn = topic['TopicArn']       #get ARN

arn = "arn:aws:sns:us-east-1:040667233965:tweets"

print(arn)

queue = sqs.get_queue_by_name(QueueName = 'tweet')
#sentiment = ["positive","negative","neutral"]

if queue:
    print("queue found!!")

es = Elasticsearch(host)
if es:
    print("elastic search host is connected...")


def worker_task():
    while True:
        msgs = queue.receive_messages(MessageAttributeNames=['Id','Tweet','Latitude','Longitude'])

        sns.subscribe(
                        TopicArn = arn,
                        Protocol = 'http',
                        Endpoint = 'http://twittmap-env.3njd2hppbi.us-east-1.elasticbeanstalk.com/'
                    )
        if len(msgs)>0:
            for message in msgs:
                id = message.message_attributes.get('Id').get('StringValue')
                tweet = message.message_attributes.get('Tweet').get('StringValue')
                lat = message.message_attributes.get('Latitude').get('StringValue')
                lon = message.message_attributes.get('Longitude').get('StringValue')
                #print(tweet)
                try:
                    response = nlu.analyze(text = tweet,features= [features.Sentiment()])
                    print(response)
                    s = json.dumps(response)
                    r = json.loads(s)
                    sentiment = r['sentiment']['document']['label']
                    senti_score = r['sentiment']['document']['score']
                    #sentiment = "pos"
                    #senti_score = 0.5
                    sns_message = {'id':id, 'tweet':tweet, 'lat': lat, 'lon': lon, 'senti': sentiment, 'score': senti_score }
                    print("SNS message:"+ str(sns_message))
                    sns.publish(TopicArn= arn , Message=json.dumps({'default':json.dumps(sns_message)}))

                except Exception as e:
                    print("ERROR:"+ str(e))

        else:
            time.sleep(1)

pool = multiprocessing.Pool(10,worker_task(), (queue,))

worker_task()
while True:
    pass