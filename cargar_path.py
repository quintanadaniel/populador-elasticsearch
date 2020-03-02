#!/usr/bin/env python3
#-*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers, exceptions
from datetime import datetime
import os, uuid


# create a new instance of the Elasticsearch client class
elastic_up_test_v6 = Elasticsearch("http://10.10.16.144:9200")

elastic_down_test_v1 = Elasticsearch("http://10.10.18.151:9200")

elastic_down_prod_v1 = Elasticsearch("http://elastic-buscadores-web.qro.local:9200")



indice_prod = ["epg_v3"]
indice_test = ["events_v1"]

def getEventsProd(id_channel):

    body_contenido_channel_down = {
                                "size": 100,
                                "from": 10,
                                "query": {
                                    "match_all": {}
                                },
                                "filter": {
                                    "bool": {
                                    "must": {
                                        "term": {
                                        "CHANNEL_ID": id_channel
                                        }
                                    }
                                    }
                                }
                                }
    
    
    res_getevent_prod = elastic_down_prod_v1.search(index=indice_prod,body=body_contenido_channel_down)
    count_doc = 0
    #print("respuesta_indice: ",res_getevent_prod)
        
    for hits_getevent_prod in res_getevent_prod['hits']['hits']:
        count_doc = count_doc +1
        
        #total de documentos por ID CHANNEL
        #print("respuesta_cantidad_documentos: ",res_getevent_prod['hits']['total'])

        new_index_events = {'ID':hits_getevent_prod['_source']['ID']
                            ,'EVENT_ID':hits_getevent_prod['_source']['EVENT_ID']
                            ,"PROVIDER_METADATA_ID":hits_getevent_prod['_source']['PROVIDER_METADATA_ID']
                            ,"LIVEREF":hits_getevent_prod['_source']['LIVEREF']
                            ,'NOMBRE':{
                                        'ESP':hits_getevent_prod['_source']['NOMBRE_ESP']
                                        ,'POR':hits_getevent_prod['_source']['NOMBRE_POR']
                                        ,'ING':hits_getevent_prod['_source']['NOMBRE_ING']}
                            ,'DESCRIPCION':{
                                            'ESP':hits_getevent_prod['_source']['DESCRIPCION_ESP']
                                            ,"POR":hits_getevent_prod['_source']['DESCRIPCION_POR']
                                            ,'ING':hits_getevent_prod['_source']['DESCRIPCION_ING']}
                            ,'INFO_CHANNEL':{
                                            'ID':hits_getevent_prod['_source']['CHANNEL_ID']
                                            ,'NOMBRE': {
                                                        'ORIGINAL':hits_getevent_prod['_source']['CHANNEL_NAME']}}
                            ,'FECHA_ACTUALIZACION':hits_getevent_prod['_source']['BD_CREATIONTIME']
                            ,'INFO_EPG':hits_getevent_prod['_source']['INFO_EPG']
                            ,'INFO_REGION':hits_getevent_prod['_source']['INFO_REGION']}
        
        #print(count_doc , new_index_events)

        res_insert = elastic_up_test_v6.exists(index=indice_test,id=hits_getevent_prod['_id'],doc_type=hits_getevent_prod['_type'])
        if res_insert == False:
            print(count_doc + hits_getevent_prod['_id'] + " registro no existe, se procede a insertarlo")
            elastic_up_test_v6.index(index=indice_test,doc_type=hits_getevent_prod['_type'],id=hits_getevent_prod['_id'],body=new_index_events)
        else:
            print(count_doc + hits_getevent_prod['_id'] + " registro ya existe ")
        
    
    

def getPrincipalEventsProd():
    query_events_prod = {
                            "_source": [
                                "CHANNEL_ID"
                            ],
                            "size": 0,
                            "from": 0,
                            "query": {
                                "match_all": {}
                            },
                            "filter": {
                                "bool": {
                                "must": [
                                    {
                                    "range": {
                                        "INFO_EPG.BEGINTIME": {
                                        "gte": "2020/03/02 00:00:00"
                                        }
                                    }
                                    }
                                ]
                                }
                            },
                            "aggs": {
                                "canales": {
                                "terms": {
                                    "field": "CHANNEL_ID",
                                    "size": 2150
                                }
                                }
                            }
                        }

    res_es_down_prod = elastic_down_prod_v1.search(index=indice_prod,body=query_events_prod)

    #print(res_es_down_prod)
    count_channel = 0
    for hit_channels_id in res_es_down_prod['aggregations']['canales']['buckets']:
        count_channel = count_channel + 1
        print(count_channel," channel_id ",hit_channels_id['key'])
        # invocamos el metodo que tra todo los contenidos por ID agrupado
        getEventsProd(hit_channels_id['key'])
        



    
getPrincipalEventsProd()

