
###Publishing from API
project="qtran-data228-project"
topic_in_api="data228_api" #publish topic for the API
apikey="6f2fe1930172af53d9f7a7a93a18d870"
lat=37.3382 #set the center location for API call
long=-121.8863

location="San Jose"

## Publishing from Storage code

bucket_name="qtran_data228_project" #very important
topic_in_batch="data228_batch" #publish topic for batch

### Pipeline module
region="us-west1"
staging_location='gs://qtran-data228-hw8/staging' 
temp_location='gs://qtran-data228-hw8/temp' 

pipeline_pub_in_api='projects/qtran-data228-project/topics/data228_api' #topic url for publish
pipeline_pub_in_batch='projects/qtran-data228-project/topics/data228_batch'  #topic url for publish 
bqtable='qtran-data228-project:data228_hw8.weather'
