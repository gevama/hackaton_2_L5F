#L5F 
#Hackaton 2 

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.options.pipeline_options import SetupOptions

from elasticsearch import Elasticsearch 

import json
import utm


class TrafficStatus(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        

        item = json.loads(element)
        print(item)

        street=item['coordinates']
        street=street[1:-1]    
        street_list=street.split('],')  
        street_list_c=[]

        for i in street_list:    
            i=i.replace("]","")
            i=i.replace("[","")
            i_list=list(i.split(","))
            lat_c=i_list[0]
            lon_c=i_list[1]
            lat,lon=utm.to_latlon(float(lat_c),float(lon_c),30,'U')
            
            i_c=[lon,lat]
           
            street_list_c.append(i_c)
        print(">>>>>>>>> Street_list_c: ")
        print(street_list_c)
        item['coordinates']=street_list_c
        
        if not item['estado'].isnumeric():
            item['estado']=0
        else: 
            item['estado']=int(item['estado'])
        
        
        return [{'idtramo':item['idtramo'],
                 'denominacion':item['denominacion'],
                 'modified':item['modified'],
                 'estado':item['estado'],   #Tipo estado: 0-fluido, 1-denso, 2-congestionado, 3-cortado
                 'location':{"type":"linestring", "coordinates":street_list_c},
                 'uri':item['uri']
                 }]


class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='trafico_tr',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  parser.add_argument('--input_topic',
                      dest='input_topic',

                      default='projects/hackaton-5f/topics/trafico_tr',
                      help='Input file to process.')
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                 
                      default='projects/hackaton-5f/subscriptions/streming6',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)


  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  google_cloud_options.project = 'hackaton-5f'
  google_cloud_options.job_name = 'Myjob5'
 
 
  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  StTraffic = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

 
  
  
  StTraffic = ( StTraffic | beam.ParDo(TrafficStatus()))
  
  StTraffic | 'Print Quote' >> beam.Map(print)
  
  StTraffic | 'Traffic Status' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()