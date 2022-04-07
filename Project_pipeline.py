from google.cloud import pubsub
from google.cloud import storage
import apache_beam as beam  #pip install apache_beam[gcp]
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam import window
import google.auth
import json 
from  datetime import datetime
import configs
import re

##pipeline options
options=pipeline_options.PipelineOptions(flags=['--streaming'])
options.view_as(pipeline_options.StandardOptions).streaming=True
_, options.view_as(GoogleCloudOptions).project=google.auth.default()

#options.view_as(GoogleCloudOptions).job_name="HW8 Streaming"
options.view_as(GoogleCloudOptions).region=configs.region
options.view_as(GoogleCloudOptions).staging_location=configs.staging_location
options.view_as(GoogleCloudOptions).temp_location=configs.temp_location

 
#Define the pub topics
topic_in_api=configs.pipeline_pub_in_api
topic_in_batch=configs.pipeline_pub_in_batch

def csvtojson(x): #converting CSV to JSON
    j_file={}
    x=x.split(',')     
    j_file['lat']=x[2]
    j_file['long']=x[3]
    j_file['location']=x[1]
    j_file['date']=x[5]
    j_file['temp']=x[7]
    j_file['rain']=x[6]
    return j_file

def apijson(weather):
    timestamp=datetime.fromtimestamp(weather['daily'][0]['dt'])
    japi={}
    japi['Coordinates_lat']=weather['lat']
    japi['Coordinates_long']=weather['lon']
    japi['Location_POI']=configs.location #weather['timezone']
    japi['Forecast_Date']= timestamp.strftime('%Y-%m-%d')
    japi['Temperature']=weather['daily'][0]['temp']['day']
    japi['Rain']= -1
    return japi


def convertdate(line):
    re_api=r'\d{4}-\d{1,2}-\d{1,2}'
    re_batch=r'\d{1,2}/\d{1,2}/\d{4}'
    if re.match(re_batch, line['date']):
        cols=line['date'].split('/')
        if len(cols[0])<2:
            cols[0]=f'0{cols[0]}'
        line['date']=f'{cols[2]}-{cols[0]}-{cols[1]}'
        return line
    elif re.match(re_api,line['date']): #ensure api is in correct date format
        return line
    else: #date is wrong format or none exists
        line['date']='0000-00-00'
        return line

def nullcheck(x): #deals with null values in batch file
    if(( x['temp'] == "") and (x['rain'] == '')):
        g={'Coordinates_long':x['long'],'Coordinates_lat':x['lat'],'Location_POI':x['location'],\
        'Forecast_Date': x['date']}
    elif x['temp'] =='':
        g={'Coordinates_long':x['long'],'Coordinates_lat':x['lat'],'Location_POI':x['location'],\
        'Forecast_Date': x['date'], 'Rain': x['rain']}
    elif x['rain'] == '':
        g={'Coordinates_long':x['long'],'Coordinates_lat':x['lat'],'Location_POI':x['location'],\
        'Forecast_Date': x['date'],'Temperature':x['temp']}
    else:
        g={'Coordinates_long':x['long'],'Coordinates_lat':x['lat'],'Location_POI':x['location'],\
        'Forecast_Date': x['date'],'Temperature':x['temp'], 'Rain': x['rain']}
    return g


with beam.Pipeline(options=options) as pipeline:
    batch_data = (pipeline | "Read from pubsub batch" >> beam.io.ReadFromPubSub(topic=topic_in_batch)
                           | "Set working window batch" >> beam.WindowInto(window.Sessions(30))
                           | "Decode from utf-8 batch">>beam.Map(lambda x: x.decode('utf-8'))
                           | "Convert csv to Json" >> beam.Map(csvtojson)
                           | "Convert mm/dd/YY to YY-mm-dd" >> beam.Map(convertdate)
                           | 'format to dictionary' >> beam.Map(nullcheck)                         
                           )
                            
   
    
    
    #batch_data=(batch_data | "Set window session" >> beam.WindowInto(window.Sessions(30))) #30
    api_data= (pipeline | "Read from pubsub api" >> beam.io.ReadFromPubSub(topic=topic_in_api)
                        | "Set working window api" >> beam.WindowInto(window.Sessions(30))
                        | "Decode from utf-8 api">>beam.Map(lambda x: x.decode('utf-8'))
                        | "Convert from pubsub" >> beam.Map(lambda x: json.loads(x))
                        | "Extract from API" >> beam.Map(apijson)
                        )
    merged= ((batch_data, api_data)
                        | "Merge collections" >> beam.Flatten()
                        | 'Write to bigquery' >> beam.io.WriteToBigQuery(configs.bqtable,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))    
    DataflowRunner().run_pipeline(pipeline,options=options)
