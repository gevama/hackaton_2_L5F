#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
""
Created on Tue Jan 21 14:32:55 2020

@author: edem
"""


from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

from apache_beam.options.pipeline_options import SetupOptions

from elasticsearch import Elasticsearch 

import utm
import json



def convierteutm(x,y):
    new_coord = utm.to_latlon(x, y, 30, 'U')   #the "30" belong to the zone of spain as the "U" value
    return(str(new_coord[0])+","+str(new_coord[1]) )

class LocationConcat(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
       
     
        item = json.loads(element)
        return [{'Identificador':item['punto_medida'],
                 'angulo':item['angulo'],
                 'intensidad':item['intensidad'],
                 'modified':item['modified'],
                 'location':(convierteutm(float(item['ycoord']),float(item['xcoord'])))
                          #here is where the function is use          
                 }]


class IndexDocument(beam.DoFn):
   
      
 
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='medicion_bicis',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackaton-5f/topics/medicion_bicis',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackaton-5f/subscriptions/streaming4',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackaton-5f'
  google_cloud_options.job_name = 'myjob4'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  medicion_bicis = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  medicion_bicis  = ( medicion_bicis  | beam.ParDo(LocationConcat()))
  
  medicion_bicis  | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  medicion_bicis  | 'Medicion bicis' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()