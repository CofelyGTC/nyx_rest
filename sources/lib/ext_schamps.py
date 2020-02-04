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

    @api.route('/api/v1/schamps/getProductionResult')
    @api.doc(description="Return the production result index",params={'token': 'A valid token'})

    class schampsGetProductionResult(Resource):    
        @token_required()
        @api.doc(description="Get day order.",params={'demandor': 'A valid User ID', 'category': 'A valid products category'})
        def get(self, user=None):
            logger.info("schamps - get order list")

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
                                        "dateOrder": {
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

            