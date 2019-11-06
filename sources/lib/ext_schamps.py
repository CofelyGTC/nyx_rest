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
