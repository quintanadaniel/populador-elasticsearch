from elasticsearch import Elasticsearch

es = Elasticsearch("http://10.10.16.144:9200/")

def getEventsId():
    
    indice = ["events_v1"]
    doc = ["event"]

    body_events_id = { "_source": "ID", 
                        "size":250,
                        "query": {
                            "match": {
                            "NOMBRE.ESP": "los simps"
                            }
                        },
                        "post_filter": {
                            "bool": {
                            "must": {
                                "term": {
                                "INFO_REGION": "mx"
                                }
                            }
                            }
                        }
                        }

    count = 0
    res_events_id = es.search(index=indice,body=body_events_id)

    for hit_id_events in res_events_id['hits']['hits']:
        count = count + 1

        print("indice ",hit_id_events['_index'])
        print("doc_type ",hit_id_events['_type'])
        print(count," ",hit_id_events['_source']['ID'])

        ind = hit_id_events['_index']
        docu = hit_id_events['_type']
        del_id = hit_id_events['_source']['ID']

        #aqui borramos todos los id de los contenidos consultados arriba.
        es.delete(index=ind,doc_type=docu,id=del_id)

getEventsId()