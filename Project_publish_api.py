#Grab from API
from google.cloud import pubsub
from google.cloud import storage
import json
import requests
import configs 
#####
publisher=pubsub.PublisherClient()

#set parameters for module for publishing
project= configs.project
topic=configs.topic_in_api
topic_path=publisher.topic_path(project, topic)

#API parameters
APIkey=configs.apikey


#grab api using OneCall
#Calling daily temperature 
Lat= configs.lat #37.83
Long= configs.long #-121.96
response=requests.get(f"https://api.openweathermap.org/data/2.5/onecall?lat={Lat}&lon={Long}&appid={APIkey}&units=imperial")
weather=response.json() 



publisher.publish(topic_path,bytes(json.dumps(weather),'utf-8'))