from elasticsearch import Elasticsearch
import json
import sys

index_name = 'new-twitter-idx'

def populate_database(file_name, index_name, host):
   # if not host:
       # es = Elasticsearch() #by default we connect to localhost:9200
   # else:
    es = Elasticsearch(host)

    file = open(file_name,'r')

    #Creating Template

    mapping = {"mappings": {
            "tweet": {
                "properties": {
                    "text": {
                        "type": "string"
                    },
                    "coordinates": {
                            "type": "geo_point"
                     }
                }
            }
    }
    }

    es.indices.create(index = index_name, body=mapping, ignore=400)
    for line in file:
        if line.strip() == "":
            continue
        try:
            data = json.loads(line)
            tweet_text = data['text']
            tweet_id = data['id']
            Long = None
            Lat = None
            if data['coordinates']:
                Long = float(data['coordinates']['coordinates'][0])
                Lat = float(data['coordinates']['coordinates'][1])
            elif 'place' in data.keys() and data['place']:
                Long = float(data['place']['bounding_box']['coordinates'][0][0][0])
                Lat = float(data['place']['bounding_box']['coordinates'][0][0][1])
            elif 'retweeted_status' in data.keys() and 'place' in data['retweeted_status'].keys() and data['retweeted_status']['place']:
                Long = float(data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][0])
                Lat = float(data['retweeted_status']['place']['bounding_box']['coordinates'][0][0][1])
            elif 'quoted_status' in data.keys() and 'place' in data['quoted_status'].keys() and data['quoted_status']['place']:
                lon = float(data['quoted_status']['place']['bounding_box']['coordinates'][0][0][0])
                lat = float(data['quoted_status']['place']['bounding_box']['coordinates'][0][0][1])
            if Lat and Long:
                es.index(index=index_name, id=tweet_id, doc_type="tweet",body={"tweet": tweet_text, "location": {"lat": Lat, "lon": Long}})
        except Exception as e:
            print("ERROR: " + str(e))
    file.close()

populate_database("FinalTweets.txt", index_name, "https://search-new-twitter-i2pdnr6a2chkajagb2cm5ggd2y.us-west-2.es.amazonaws.com")
                              

