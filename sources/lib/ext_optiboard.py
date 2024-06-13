import os
import json
import re
import time
import uuid

import arrow
import datetime
import logging
import requests
import feedparser
from os import listdir
from flask import make_response,url_for
from flask import Flask, jsonify, request,Blueprint
from flask_restx import Namespace, Api, Resource, fields

print("***>"*100)

logger=logging.getLogger()

# api = Namespace('optiboard', description='Optiboard APIs')

def get_carousel_config(es,opti_id):    
    config = None
    res    = None    
    try:
        logger.info('TRY to retrieve a carousel with this name: '+str(opti_id)+' ...')
        res = es.search(index="nyx_carousel", query={"match":{"name.keyword":{"query":str(opti_id)}}})

        if res is None or res['hits']['total']['value'] == 0:
            raise Exception("carousel not found by name")


    except:
        logger.info('... failed !')
        try:
            logger.info('TRY to retrieve a carousel with this id: '+str(opti_id)+' ...')
            res = config=es.get(index="nyx_carousel",id=str(opti_id))
        except:
            logger.info('... failed !')
            res = None
            
    
    if res is not None and res['hits']['total']['value'] > 0:
        car = res['hits']['hits'][0]
        id_view_array = []
        
        for i in car['_source']['id_array']:
            id_view_array.append(i['id'])


        res_views = es.mget(index = 'nyx_view_carousel',
                                   body = {'ids': id_view_array})

        _source = {
            'pages': []
        }

        for i in res_views['docs']:
            page = {}
            if '_source' in i:
                for j in i['_source']:
                    #print(j)
                    page[j] = i['_source'][j]
                page["_id"] = i["_id"]
                _source['pages'].append(page)

        #print(_source)
        
        config = {
            '_index': 'nyx_carousel',
             '_type': 'doc',
             '_id': str(opti_id),
             '_version': 17,
             'found': True,
             '_source': _source 
        }        
    else:
        try:
            logger.info('TRY to retrieve a carRousel with this id: '+str(opti_id)+' ...')
            config=es.get(index="nyx_carrousel",id=str(opti_id))
        except:
            logger.info('... failed !')
            return None
        
    return config["_source"]["pages"]



def loadRss(url):
    logger.info("LOAD RSS:"+url)
    NewsFeed = feedparser.parse(url)
    rsstitles=[]
    for entry in NewsFeed.entries:
        rsstitles.append(entry["title"])
    return rsstitles

def get_weather(api_key, language,location, es):
    url = "https://api.openweathermap.org/data/2.5/forecast?q={}&lang={}&units=metric&appid={}".format(location, language,api_key)
    r = requests.get(url)
    weather= r.json()
    weather["list"][0]["main"]["temp"]=int((weather["list"][0]["main"]["temp_min"]+weather["list"][0]["main"]["temp_max"])/2)
    weather["list"][8]["main"]["temp"]=int((weather["list"][8]["main"]["temp_min"]+weather["list"][8]["main"]["temp_max"])/2)

    for x in [0,8]:
        logo = es.search(index="meteo_logo", body={
            "query": {
                "match": {
                    "_id": weather["list"][x]["weather"][0]["icon"]
                }
            },
            "size": 1
        })
        if logo and logo["hits"]["hits"] and logo["hits"]["hits"][0]["_source"].get("logo_name"):
            baseURL = os.environ["UI_BASE_URL"].split('#')[0]
            if not baseURL.endswith('/'): baseURL += "/"
            weather["list"][x]["main"]["url"]=baseURL+"public/meteo/"+logo["hits"]["hits"][0]["_source"]["logo_name"]+".png"
        else:
            weather["list"][x]["main"]["url"]="https://openweathermap.org/img/w/"+weather["list"][x]["weather"][0]["icon"]+".png"
    

    retobj={"today":weather["list"][0],"tomorrow":weather["list"][8]}
    
    return retobj    


def config(api,conn,es,redis,token_required):
    logger.info(redis)

    #---------------------------------------------------------------------------
    # API LIFE SIGN
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/lifesign')
    @api.doc(description="Optiboard. Life Sign")
    class optiboard(Resource):                    
        def post(self):
            logger.info("Optiboard lifesign. Client.")

            req= json.loads(request.data.decode("utf-8"))
            print('req: ', req)

            guid=req["config"]["guid"]
            disk=req.get("disk",{})
            version=req.get("version","NA")
            starttime=req.get("starttime","")
            logger.info("===>"+guid+"<===")
            
            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    if record["accepted"]==1:
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        return {'error':""}            
                except:
                    logger.info("Record does not exists. Returning error.")     
                    return {'error':"KO"}
            
            redopti=redis.get("optiboard_"+guid)
            redoptiobj=json.loads(redopti)
            redoptiobj["starttime"]=starttime
            redoptiobj["version"]=version
            redoptiobj["disk"]=disk
            redoptiobj["@timestamp"]=arrow.utcnow().isoformat().replace("+00:00", "Z")
            redoptiobj["station"]=redoptiobj["optiboard"]

            index='optiboard_status-'+datetime.datetime.now().strftime('%Y.%m')
            es.index(index=index,document=redoptiobj)            

            return {'error':""}   

    #---------------------------------------------------------------------------
    # API POLL
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/poll')
    @api.doc(description="Optiboard. Poll End point", params={'guid': 'A guid'})
    class optiboard(Resource):                    
        def get(self):
            logger.info("Optiboard poll called. Client.")
            
            guid=request.args["guid"]
            logger.info("===>"+guid+"<===")

            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    logger.info("1")
                    if record["accepted"]==1:
                        logger.info("2")
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        logger.info("3")
                        return {'error':""}            
                except Exception as e:
                    logger.info("Record does not exists. Returning error.",exc_info=True)     
                    return {'error':"KO"}

            res=es.search(index="optiboard_command",
                    size=200,
                    query={"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"term":{"guid.keyword":{"value":guid}}},{"term":{"executed":{"value":0,"boost":1.0}}}]}}]}}]},
                           }
                    )
            logger.info(res)



            commands=[]

            if "hits" in res and "hits" in res["hits"] and len (res["hits"]["hits"]) >0:
                commands=[x["_source"] for x in res["hits"]["hits"]]

                execTime=arrow.utcnow().isoformat().replace("+00:00", "Z")            

                q = {
                        "script": {
                            "inline": "ctx._source.executed=1;ctx._source.execTime=\""+execTime+"\"",
                            "lang": "painless"
                        },
                        "query": {"bool":{"filter":[{"bool":{"must":[{"bool":{"must":[{"term":{"guid.keyword":{"value":guid}}},{"term":{"executed":{"value":0,"boost":1.0}}}]}}]}}]}}                    
                    }
                    

                res2=es.update_by_query(body=q, index='optiboard_command')
                logger.info(res2)

            return {'error':"OK","commands":commands} 
                   
    #---------------------------------------------------------------------------
    # API GETCAROUSEL
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/getcarousel')
    @api.doc(description="User get carousel", params={'carousel': 'A carousel id'})
    class optiboardGetCarousel(Resource):
        def get(self):
            logger.info("User get carousel.")
            carouselID=request.args["carousel"]
            try:
                carousel=es.get(index="nyx_carousel",id=carouselID)
                carousel=carousel["_source"]
                logger.info(carousel)
                record = {}
                if "id_array" in carousel:
                    if "rss" in record:
                        record["rss_feed"]=loadRss(record["rss"])
                    if "weather" in record:
                        record["language"]= record["weather"]["language"]
                        record["weather"]=get_weather(record["weather"]["apikey"], record["weather"]["language"],record["weather"]["location"], es)
                    res_views = es.mget(
                        index = 'nyx_view_carousel',
                        body = {'ids': [ids["id"] for ids in carousel["id_array"]]}
                    )
                    record["carrousel"]=res_views
                    pages = []
                    for i in res_views['docs']:
                        page = {}
                        if '_source' in i:
                            for j in i['_source']:
                                #print(j)
                                page[j] = i['_source'][j]
                            page["_id"] = i["_id"]
                            pages.append(page)
                    record["carrousel"]=pages
                return {'error':"",'rec':record}
            except:
                logger.info("Not carousel find.")
                return {'error':"Not carousel find.",'errorcode':500}  

    #---------------------------------------------------------------------------
    # API GETWEATHER
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/getweather')
    @api.doc(description="User get carousel", params={'guid': 'GUID'})
    class optiboardGetWeather(Resource):
        def get(self):
            logger.info("Optiboard getweather called.")
            guid=request.args["guid"]
            logger.info(guid)
            try:
                record=es.get(index="optiboard_token",id=guid)
                record=record["_source"]
            except:
                logger.info("Record does not exists.")
                return {'error':"Unknown error",'errorcode':99}   
            weather = get_weather(record["weather"]["apikey"], record["weather"]["language"],record["weather"]["location"], es)
            return {'error':"",'rec':weather}

    #---------------------------------------------------------------------------
    # API GETCONFIG
    #---------------------------------------------------------------------------
    getConfigAPI = api.model('getConfig_model', {
        'guid': fields.String(description="A screen guid", required=False)
    })
    @api.route('/api/v1/ext/optiboard/getconfig')
    @api.doc(description="Optiboard get Config")
    class optiboardGetConfig(Resource):   
        @api.expect(getConfigAPI)                 
        def post(self):
            logger.info("Optiboard config called. ")            
            req= json.loads(request.data.decode("utf-8"))  
            logger.info(req)
            guid=req["guid"]
            try:
                record=es.get(index="optiboard_token",id=guid)
                record=record["_source"]
            except:
                logger.info("Record does not exists. Creating it.")
                newrecord=req
                newrecord["accepted"]=0
                newrecord["@creationtime"]=arrow.utcnow().isoformat().replace("+00:00", "Z")
                es.index(index="optiboard_token",id=guid,body=newrecord)
                record=newrecord
            if record["accepted"]==1:
                logger.info("Record is valid")
                if "rss" in record:
                    record["rss_feed"]=loadRss(record["rss"])
                if "weather" in record:
                    record["language"]= record["weather"]["language"]
                    record["weather"]=get_weather(record["weather"]["apikey"], record["weather"]["language"],record["weather"]["location"], es)
                if "carrousel" in record:
                    record["carrousel"]=get_carousel_config(es,record["carrousel"])
                    if record['carrousel'] is None: 
                        return {'error':"Unable to retrieve carousel",'errorcode':101}
                return {'error':"",'rec':record}            
            else:
                return {'error':"Waiting for approval",'errorcode':100}            
            return {'error':"Unknown error",'errorcode':99}
    
    #---------------------------------------------------------------------------
    # API SETCOUNTUSER
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/setcountuser')
    @api.doc(description="Optiboard get Config")
    class devices(Resource):
        def post(self):
            body = request.json.get("body")
            guid = body.get('guid')
            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    logger.info("1")
                    if record["accepted"]==1:
                        logger.info("2")
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        logger.info("3")
                        return {'error':""}            
                except Exception as e:
                    logger.info("Record does not exists. Returning error.",exc_info=True)     
                    return {'error':"KO"}
                
            index = request.json.get("index")
            if index in ["optiboard_count_user", "optiboard_count_click"]:
                if not body.get("@timestamp"): body["@timestamp"] = datetime.utcnow().isoformat() + 'Z'
                logger.info(f'body: {body}')
                sea = es.search(index="optiboard_token", body={
                    "query": {
                        "match": {
                            "guid": body.get('guid')
                        }
                    },
                    "size": 1
                })
                config = sea["hits"]["hits"][0]["_source"]
                body["client"] = config['client']
                body["optiboard"] = config['optiboard']
                body["description"] = config['description']
                if body.get("_id"): del body["_id"]
                es.index(index=index, document=body)
                es.update_by_query(
                    index = index, 
                    body = {
                        "script": {
                            "source": f"ctx._source['optiboard'] = '{config['optiboard']}'; ctx._source['client'] = '{config['client']}'; ctx._source['description'] = '{config['description']}'",
                            "lang": "painless"
                        },
                        "query": {
                            "match_phrase": {
                                "guid": body.get('guid')
                            }
                        }
                    }
                )
                return 'ok', 200
            else:
                return 'index non reconnu', 404


    @api.route('/api/v1/ext/optiboard/sendusagecount')
    class sendusagecount(Resource):
        def post(self):
            logger.info(">>>>> Optiboard/sendusagecount")
            print('request.json: ', request.json)
            body = request.json.get("body")
            guid = request.json.get("guid")
            if redis.get("optiboard_"+guid)==None:
                logger.info("Token does not exists in redis")

                try:
                    record=es.get(index="optiboard_token",id=guid)
                    logger.info(">="*30)
                    
                    record=record["_source"]
                    logger.info("1")
                    if record["accepted"]==1:
                        logger.info("2")
                        redis.set("optiboard_"+guid,json.dumps(record),300)
                        logger.info("3")
                        return {'error':""}            
                except Exception as e:
                    logger.info("Record does not exists. Returning error.",exc_info=True)     
                    return {'error':"KO"}
                
            if body:
                log_data_list = body.split('\n')
                for line in log_data_list:
                    # Faites ce que vous voulez avec chaque ligne, par exemple l'imprimer
                    log_pattern = r'\[(.*?)\] \[(.*?)\] (.*?) - (.*)'

                    # Extraire les informations de la ligne de log
                    match = re.match(log_pattern, line)

                    if match:
                        timestamp_str = match.group(1)
                        log_level = match.group(2)
                        log_body_str = match.group(4)
                        data = json.loads(log_body_str)
                        index = data.get("index")
                        body = data.get("body")
                        # Vérifiez si l'index existe
                        if es.indices.exists(index=index):
                            try:
                                existing_doc = sea = es.search(index=index, body={
                                    "query": {
                                        "bool": {
                                            "must": [
                                                {"match": {"guid": body.get('guid')}},
                                                {"match": {"@timestamp": body.get('@timestamp')}}
                                            ]
                                        }
                                    },
                                    "size": 1
                                })
                                if not existing_doc["hits"]["hits"]: existing_doc = None
                            except: existing_doc = None
                        else:
                            existing_doc = None

                        if not existing_doc:
                            sea = es.search(index="optiboard_token", body={
                                "query": {
                                    "match": {
                                        "guid": body.get('guid')
                                    }
                                },
                                "size": 1
                            })
                            config = sea["hits"]["hits"][0]["_source"]
                            body["client"] = config['client']
                            body["optiboard"] = config['optiboard']
                            body["description"] = config['description']
                            if body.get("_id"): del body["_id"]
                            if body.get("mode"): del body["mode"]
                            logger.info(f'body: {body}')
                            res = es.index(index=index, document=body)
                            logger.info(f'res: {res}')
                            # time.sleep(0.1)
            return 'ok', 200


    #---------------------------------------------------------------------------
    # API GETTOKEN
    #---------------------------------------------------------------------------
    @api.route('/api/v1/ext/optiboard/gettoken')
    @api.doc(description="Optiboard get token", params={'guid': 'A guid'})
    class optiboardGetToken(Resource):                    
        def get(self):
            logger.info("Optiboard get token called. ")  

            guid=request.args["guid"]
            logger.info("GUID="+guid)  
            try:
                record=es.get(index="optiboard_token",id=guid)
                record=record["_source"]
                token=uuid.uuid4()
                if record["accepted"]==1:
                    redis.set("nyx_tok_"+str(token),json.dumps({'return':"OK", 'device': 'optiboard', "login": guid}),3600*24)
                    resp=make_response(jsonify({'error':"","token":token}))
                else:
                    resp=make_response(jsonify({'error':"KO"}))
                #resp.set_cookie('nyx_kibananyx', str(token))                
                
                return resp
            except Exception as e:
                logger.info("Record does not exists. Ignoring request.",exc_info=1)
            return {'error':"Unknown error",'errorcode':99}