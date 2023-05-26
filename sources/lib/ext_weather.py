import logging
from flask_restx import Namespace,Api, Resource, fields
from flask import Flask, jsonify, request
import json
import os
from datetime import datetime
from elastic_helper import es_helper
from datetime import timedelta
import pandas as pd


print("***>"*100)

logger=logging.getLogger()

def getLastUpdate(es):
    end = datetime.now()
    start = end-timedelta(days=3)
    df  = es_helper.elastic_to_dataframe(es, 'weather_real', start=start, end=end)
    df = df.sort_values(by="@timestamp", ascending=False).reset_index(drop=True)

    value = df.at[0, "@timestamp"]

    return int(value/60000)

def getTagWeather(es, city,table,tag,time):
    query='cpost: '+city
    value = 0
    
    if table == 'pred':
        start = datetime.now()
        end = start+timedelta(hours=time)
        df = es_helper.elastic_to_dataframe(es, 'weather_prediction', start=start, end=end, query=query)
        df = df.sort_values(by="@timestamp", ascending=False).reset_index(drop=True)
        logger.info(df)
        value = df.at[0, tag]
        logger.info(value)
        
    else:
        end = datetime.now()
        start = end-timedelta(hours = 3)
        df = pd.DataFrame()
        
        
        if time == 0:
            df = es_helper.elastic_to_dataframe(es, 'weather_real', start=start, end=end, query=query)
            df = df.sort_values('@timestamp', ascending=False).reset_index(drop=True)
            logger.info(df)
            value = df.at[0, tag]
        else:
            start = end-timedelta(hours=time)
            df = es_helper.elastic_to_dataframe(es, 'weather_real', start=start, end=end, query=query)
            df = df.sort_values('@timestamp').reset_index(drop=True)
            value = df.at[0, tag]
            
            
    return str(value)

def getLogoWeather(es, city,table,time):
    query='cpost: '+city
    value = 1
    
    if table == 'pred':
        start = datetime.now()
        end = start+timedelta(hours=time)
        df = es_helper.elastic_to_dataframe(es, 'weather_prediction', start=start, end=end, query=query)
        df = df.sort_values(by="@timestamp", ascending=False).reset_index(drop=True)
        logger.info(df)
        row = df.iloc[0]
        logger.info(row)
        
        radiance = int(row['radiance'])
        symbol_cover = int(row['symbol_cover'])
        wind = int(row['wind_speed_to'])
        rain = float(row['qpcp'])
        temp = float(row['temp'])
        
        if radiance <= 750 and radiance > 350 and symbol_cover in [6,7]:
            value = 2
            if wind >= 50:
                value = 4
        elif radiance > 20  and symbol_cover in [4,5]:
            value = 3
        elif symbol_cover in [6,7] and rain >=4 and rain < 7:
            value = 5
            if temp < 2:
                value = 7
        elif rain > 7:
            value = 6
            if temp < 2:
                value = 8
        
        elif radiance == 0 and symbol_cover in [6,7]:
            value = 9
        elif radiance == 0 and symbol_cover in [4,5,6,7]:
            value = 10
            if rain > 4:
                value = 12
            if wind >= 50:
                value = 11
        elif radiance == 0 and symbol_cover in [6,7] and temp < 2 and rain > 4:
            value = 13
            if rain > 7:
                value = 14
    
            
            
    return value 



def config(api,conn,es,redis,token_required):
    #---------------------------------------------------------------------------
    # API configRest
    #---------------------------------------------------------------------------

    @api.route('/api/v1/weather/data')
    @api.doc(description="Get predictive data of weather.",params={'token':'A valid token', 'type':'pred or real', 'time':'Timedelta for prediction', 'city':'A valid City', 'tag':'The Tag to return'})
    class weather_data(Resource):
        #@token_required()
        @api.doc(description="Get predictive data of weather.",params={'token':'A valid token', 'type':'pred or real', 'time':'Timedelta for prediction', 'city':'A valid City', 'tag':'The Tag to return'})
        def get(self, user=None):
            #tokenRest = os.environ['token']
            logger.info('New WEATHER Data By Get Request')
            token=request.args.get('token')
            if token == 'GETWEATHER':
                print(conn)
                logger.info(request.args)
                type =request.args.get('type')
                timets = int(request.args.get('time'))
                city =request.args.get('city')
                tag =request.args.get('tag')
                value = getTagWeather(es, city, type, tag,timets)
                return value

            else:
                return {'error':"BAD TOKEN"}
                
    @api.route('/api/v1/weather/logo')
    @api.doc(description="Get predictive data of weather.",params={'token':'A valid token', 'type':'pred or real', 'time':'Timedelta for prediction', 'city':'A valid City', 'tag':'The Tag to return'})
    class weather_data(Resource):
        #@token_required()
        @api.doc(description="Get predictive data of weather.",params={'token':'A valid token', 'type':'pred or real', 'time':'Timedelta for prediction', 'city':'A valid City', 'tag':'The Tag to return'})
        def get(self, user=None):
            #tokenRest = os.environ['token']
            logger.info('New WEATHER Data By Get Request')
            token=request.args.get('token')
            if token == 'GETWEATHER':
                print(conn)
                logger.info(request.args)
                type =request.args.get('type')
                timets = int(request.args.get('time'))
                city =request.args.get('city')
                value = getLogoWeather(es, city, type, timets)
                return value

            else:
                return {'error':"BAD TOKEN"}            

    @api.route('/api/v1/weather/last_update')
    @api.doc(description="Get predictive data of weather.",params={'token':'A valid token'})
    class weather_data(Resource):
        #@token_required()
        @api.doc(description="Get predictive data of weather.",params={'token':'A valid token'})
        def get(self, user=None):
            #tokenRest = os.environ['token']
            logger.info('New WEATHER Data By Get Request')
            token=request.args.get('token')
            if token == 'GETWEATHER':
                print(conn)
                value = getLastUpdate(es)
                return value

            else:
                return {'error':"BAD TOKEN"}