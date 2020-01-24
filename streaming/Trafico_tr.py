#BEAM Hackathon 2
# Traffic Status

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
        
        #{"empty_slots":17,"extra":{"address":"Economista Gay - Constituci\xc3\xb3n","banking":false,"bonus":false,"last_update":1578482815000,"slots":20,"status":"OPEN","uid":136},"free_bikes":3,"id":"1f6b81722ca23ce520f77207b868afa9","latitude":39.4899091610835,"longitude":-0.375701108044157,"name":"136_CALLE_ECONOMISTA_GAY","timestamp":"2020-01-08T11:34:20.782000Z"}'
        #Example traffic status
        #{"idtramo":"442","denominacion":"SAN VICENTE (DE PL. DE LA REINA A PL. DEL AYUNTAMIENTO)","modified":"2020-01-19T12:12:04.634+01:00","estado":"0","coordinates":"[[725736.492,4372638.061],[725656.381,4372486.931],[725651.793,4372469.163]]","uri":"http://apigobiernoabiertortod.valencia.es/apirtod/datos/estado_trafico/442.json"}
        
        item = json.loads(element)
        print(item)
        #print("*************** COORDINATES ************")
        #print(item['coordinates'])
        street=item['coordinates']
        street=street[1:-1]    #Eliminar [ ] del inicio y final
        street_list=street.split('],')  #Convierto string en lista
        street_list_c=[]

        for i in street_list:     #Para cada coordenada la convierto a coordenada utm
            i=i.replace("]","")
            i=i.replace("[","")
            i_list=list(i.split(","))
            lat_c=i_list[0]
            lon_c=i_list[1]
            lat,lon=utm.to_latlon(float(lat_c),float(lon_c),30,'U')
            #i_c=str(lat)+","+str(lon)
            i_c=[lon,lat]
            #i_c=[]
            #i_c.append(lat)
            #i_c.append(lon)
            street_list_c.append(i_c)
        print(">>>>>>>>> Street_list_c: ")
        print(street_list_c)
        item['coordinates']=street_list_c
        
        if not item['estado'].isnumeric():
            item['estado']=0
        else: 
            item['estado']=int(item['estado'])
        
        #print("STREET_LIST_C: ")
        #print(street_list_c)
        
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
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackaton-5f/topics/trafico_tr',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackaton-5f/subscriptions/streming6',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackaton-5f'
  google_cloud_options.job_name = 'Myjob5'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  StTraffic = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  StTraffic = ( StTraffic | beam.ParDo(TrafficStatus()))
  
  StTraffic | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  StTraffic | 'Traffic Status' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()