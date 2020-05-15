import json
import time
import logging
import tzlocal
import calendar
import numpy as np
import pandas as pd
import elasticsearch
import dateutil.parser

from datetime import date
from datetime import datetime
from datetime import timedelta

from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields
from elastic_helper import es_helper



logger=logging.getLogger()
logger.info("***>"*100)

def config(api,conn,es,redis,token_required):

    @api.route('/api/v1/schamps/new_order')
    @api.doc(description="Create a new order",params={'token': 'A valid token'})

    class schampsNewOrder(Resource):    
        @token_required()
        @api.doc(description="Create a new order.",params={'token': 'A valid token'})
        def post(self, user=None):
            logger.info("schamps - post new order")

            
            req= json.loads(request.data.decode('utf-8'))
            conn.send_message('/queue/SCHAMPS_NEW_ORDER', json.dumps(req))


            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}
            
    @api.route('/api/v1/schamps/getDeliveredList')
    @api.doc(description="Return the delivered list index",params={'token': 'A valid token'})

    class schampsGetProductionResult(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'start': 'start of search', 'stop': 'End of search'})
        def get(self, user=None):
            logger.info("schamps  get order list")

            start=request.args.get('start')
            stop=request.args.get('stop')

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "range": {
                                        "dateDeliver": {
                                        "from": start,
                                        "to": stop,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_delivered",body=query, size=10000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}

          

    @api.route('/api/v1/schamps/getToDeliverList')
    @api.doc(description="Return the production result index",params={'token': 'A valid token'})

    class schampsGetProductionResult(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'start': 'start of search', 'stop': 'End of search'})
        def get(self, user=None):
            logger.info("schamps  get order list")

            start=request.args.get('start')
            stop=request.args.get('stop')

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "range": {
                                        "dateDeliver": {
                                        "from": start,
                                        "to": stop,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_to_deliver",body=query, size=10000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}


    @api.route('/api/v1/schamps/getProductionResult')
    @api.doc(description="Return the production result index",params={'token': 'A valid token'})

    class schampsGetProductionResult(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'start': 'start of search', 'stop': 'End of search'})
        def get(self, user=None):
            logger.info("schamps  get order list")

            start=request.args.get('start')
            stop=request.args.get('stop')

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "range": {
                                        "@timestamp": {
                                        "from": start,
                                        "to": stop,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamp_production_result",body=query, size=10000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}
    
    @api.route('/api/v1/schamps/get_products_tree')
    @api.doc(description="Return Products tree",params={'token': 'A valid token'})

    class schampsGetProductsTree(Resource):    
        @token_required()
        @api.doc(description="Get products tree.",params={'token': 'A valid token'})
        def get(self, user=None): 
            logger.info("schamps - get products tree")
            df = es_helper.elastic_to_dataframe(es, index="products_parameters_new")
            objClass = {}
            lvl1 = df .sortLvl1.unique()
            for i in lvl1:
                objClass[i] = {}
                subDF = df.loc[df['sortLvl1'] == i]
                lvlObjects = subDF.sortLvl2.unique()
                for j in lvlObjects:
                    objClass[i][j] = {}
                    subSubDF = subDF.loc[subDF['sortLvl2'] == j]
                    subLvlObjectsLvl3 = subSubDF.sortLvl3.unique()
                    objClass[i][j]['sortLvl3'] = subLvlObjectsLvl3.tolist()
                    subLvlObjectsLvl4 = subSubDF.sortLvl4.unique()
                    objClass[i][j]['sortLvl4'] = subLvlObjectsLvl4.tolist()
                    subLvlObjectsLvl5 = subSubDF.sortLvl5.unique()
                    objClass[i][j]['sortLvl5'] = subLvlObjectsLvl5.tolist()
                    subLvlObjectsLvl6 = subSubDF.sortLvl6.unique()
                    objClass[i][j]['sortLvl6'] = subLvlObjectsLvl6.tolist()
                    subLvlObjectsLvl7 = subSubDF.sortLvl7.unique()
                    objClass[i][j]['sortLvl7'] = subLvlObjectsLvl7.tolist()
                    subLvlObjectsLvl8 = subSubDF.sortLvl8.unique()
                    objClass[i][j]['sortLvl8'] = subLvlObjectsLvl8.tolist()
                    subLvlObjectsLvl9 = subSubDF.sortLvl9.unique()
                    objClass[i][j]['sortLvl9'] = subLvlObjectsLvl9.tolist()
                    subLvlObjectsLvl10 = subSubDF.sortLvl10.unique()
                    objClass[i][j]['sortLvl10'] = subLvlObjectsLvl10.tolist()
                    
                    #for k in subLvlObjects:
                    #    objClass[i][j][k] = {}
            
            req = {'results': False, 'reccords': objClass}
            logger.info(objClass)
            return {'error':"",'status':'ok', 'data': json.dumps(req)}


    @api.route('/api/v1/schamps/check_user_shop')
    @api.doc(description="Return The Shop Name attribute to the user",params={'token': 'A valid token'})

    class schampGetUserShop(Resource):    
        @token_required()
        @api.doc(description="Get User Shop.",params={'demandor': 'A valid User ID'})
        def get(self, user=None):
            logger.info("schamps - get user shop")
            demandor=request.args.get('demandor')
            

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "term": {
                                    "userId.keyword": {
                                        "value": demandor,
                                        "boost": 1.0
                                    }
                                    }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamp_link_shop_user",body=query, size=1000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}        


    @api.route('/api/v1/schamps/check_order_new')
    @api.doc(description="Create a new order",params={'token': 'A valid token'})

    class schampsGetOrderNew(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'demandor': 'A valid User ID', 'shop': 'The name of the attributed Shop'})
        def get(self, user=None):
            logger.info("schamps - get order list")
            demandor=request.args.get('demandor')
            shop = request.args.get('shop')
            

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "wildcard": {
                                        "shop.keyword": {
                                        "wildcard": shop,
                                        "boost": 1.0
                                        }
                                    }
                                    },
                                    {
                                    "range": {
                                        "dateOrder": {
                                        "from": "now/d",
                                        "to": None,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_orders",body=query, size=1000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}




    @api.route('/api/v1/schamps/get_unsales')
    @api.doc(description="Create a new order",params={'token': 'A valid token'})

    class schampsGetUnsales(Resource):    
        @token_required()
        @api.doc(description="Get last 5 days orders.",params={'demandor': 'A valid User ID', 'shop': 'The name of the attributed Shop'})
        def get(self, user=None):
            logger.info("schamps - get order list")
            demandor=request.args.get('demandor')
            shop = request.args.get('shop')
            

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "wildcard": {
                                        "shop.keyword": {
                                        "wildcard": shop,
                                        "boost": 1.0
                                        }
                                    }
                                    },
                                    {
                                    "range": {
                                        "dateOrder": {
                                        "from": "now-5d",
                                        "to": None,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_orders",body=query, size=1000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}        

    @api.route('/api/v1/schamps/check_order')
    @api.doc(description="Create a new order",params={'token': 'A valid token'})

    class schampsGetOrder(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'demandor': 'A valid User ID', 'category': 'A valid products category'})
        def get(self, user=None):
            logger.info("schamps - get order list")
            demandor=request.args.get('demandor')
            category=request.args.get('category')

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "wildcard": {
                                        "demandor.keyword": {
                                        "wildcard": demandor,
                                        "boost": 1.0
                                        }
                                    }
                                    },
                                    {
                                    "wildcard": {
                                        "category.keyword": {
                                        "wildcard": category,
                                        "boost": 1.0
                                        }
                                    }
                                    },
                                    {
                                    "range": {
                                        "dateOrder": {
                                        "from": "now/d",
                                        "to": None,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_orders",body=query, size=1000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}




    @api.route('/api/v1/schamps/check_resume_order')
    @api.doc(description="Get alls order of the day for a specific customer",params={'token': 'A valid token'})

    class schampsGetResumeOrder(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'demandor': 'A valid User ID'})
        def get(self, user=None):
            logger.info("schamps - get order list")
            demandor=request.args.get('demandor')

            query = {
                "from": 0,
                "size": 200,
                "query": {
                    "bool": {
                    "filter": [
                        {
                        "bool": {
                            "must": [
                            {
                                "bool": {
                                "must": [
                                    {
                                    "wildcard": {
                                        "demandor.keyword": {
                                        "wildcard": demandor,
                                        "boost": 1.0
                                        }
                                    }
                                    },
                                    
                                    {
                                    "range": {
                                        "dateOrder": {
                                        "from": "now/d",
                                        "to": None,
                                        "include_lower": True,
                                        "include_upper": True,
                                        "boost": 1.0
                                        }
                                    }
                                    }
                                ]
                                }
                            }
                            ]
                        }
                        }
                    ]
                    }
                }
            }

            res=es.search(index="schamps_orders",body=query, size=1000) 
            req = {'results': False, 'reccords': []}

            if res['hits']['total']['value'] != 0:
                req = {'results': False, 'reccords': res['hits']['hits']}



            
            return {'error':"",'status':'ok', 'data': json.dumps(req)}
            

            