#Grab from Batch
from google.cloud import pubsub
from google.cloud import storage
import json
import configs
import re
#####
publisher=pubsub.PublisherClient()
client=storage.Client()
#Pub sub
project= configs.project
topic=configs.topic_in_batch
topic_path=publisher.topic_path(project, topic)

#Storage
bucket=client.get_bucket(configs.bucket_name)
    

for j in client.list_blobs(bucket):
    if j.name.endswith('csv'):
       
        blob= bucket.get_blob(j.name) #get bucket data as blob
        path=blob.download_as_bytes() #download file
        path=path.decode('UTF-8') #decode from bytes
        data=path.splitlines()
      #Placement of x is dependent on local batch file
        for i in data[1:]: #skip headers
            publisher.publish(topic_path, bytes(i,"utf-8"))

