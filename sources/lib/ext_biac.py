import json
import time
import pytz
import logging
import tzlocal
import calendar
from threading import Timer
import collections
import numpy as np
import pandas as pd
import elasticsearch
import dateutil.parser

import traceback

from datetime import date
from datetime import datetime
from datetime import timedelta

from elastic_helper import es_helper 
from flask import Flask, jsonify, request
from flask_restplus import Api, Resource, fields


logger=logging.getLogger()
logger.info("***>"*100)

def config(api,conn,es,redis,token_required):

    @api.route('/api/v1/biac/kpi_model/<string:kpi>')
    @api.doc(description="Get kpi entity model.",params={'token': 'A valid token'})

    class biacKPIEntityModel(Resource):    
        @token_required()
        @api.doc(description="Get kpi entity model.",params={'token': 'A valid token'})
        def get(self, kpi, user=None):
            logger.info("biac - get kpi"+kpi+" model")
            logger.info(user)

            kpi_model = {}
            if kpi == '304': 
                kpi_model = determine_model_304(user)
            elif kpi == '600': 
                kpi_model = determine_model_600(user)
            else:
                kpi_model = retrieve_kpi_entities_model(es, user['privileges'], kpi=kpi)
            return {'error':"",'status':'ok', 'data': json.dumps(kpi_model)}
    
    @api.route('/api/v1/biac/kpi600_monthly/<string:lot>/<string:tec>/<string:date>')
    @api.doc(description="Get kpi600 monthly.",params={'token': 'A valid token'})

    class biacKPI600Monthly(Resource):    
        @token_required()
        @api.doc(description="Get kpi600 monthly record.",params={'token': 'A valid token'})
        def get(self, lot, tec, date, user=None):
            logger.info("biac - get kpi600 monthly")

            month = dateutil.parser.parse(date)
            
            return {'error':"",'status':'ok', 'data': get_kpi600_value(es, lot, tec, month)}
    
    @api.route('/api/v1/biac/kpi304_monthly/<string:lot>/<string:tec>/<string:date>')
    @api.doc(description="Get kpi304 monthly.",params={'token': 'A valid token'})

    class biacKPI304Monthly(Resource):    
        @token_required()
        @api.doc(description="Get kpi304 month records.",params={'token': 'A valid token'})
        def get(self, lot, tec, date, user=None):
            logger.info("biac - get kpi304 monthly")

            date = dateutil.parser.parse(date)
            
            return {'error':"",'status':'ok', 'data': get_kpi304_values(es, lot, tec, date)}

        def post(self, lot, tec, date, user=None):
            logger.info("biac - get kpi304 monthly")
            try:
                date = dateutil.parser.parse(date)
                update_kib_kpi304(es, lot, tec, date)
                
                return {'error':"",'status':'ok'}
            except Exception as e:
                logger.error(e)
                return {'error':str(e),'status':'ko'}
    
    post_kpi104_monthly = api.model('post_kpi104_monthly_model', {
        'last_update_time': fields.Date(description="the last update time", required=True),
    })

    @api.route('/api/v1/biac/kpi104_monthly')
    @api.doc(description="Post kpi104 monthly.",params={'token': 'A valid token'})

    class biacKPI104Monthly(Resource):    
        @token_required()
        @api.doc(description="Post biac kpi 104 monthy.",params={'token': 'A valid token'})
        @api.expect(post_kpi104_monthly)
        def post(self, user=None):
            logger.info("biac - post kpi 104 monthly")
            req= json.loads(request.data.decode("utf-8"))   
            last_update_time = dateutil.parser.parse(req['last_update_time'])

            update_kpi104_monthly(es, last_update_time)

            return {'error':"",'status':'ok'}

    post_kpi101_monthly = api.model('post_kpi101_monthly_model', {
        'month_to_update': fields.Date(description="the month to update", required=True),
        'number_of_call_1': fields.String(description="number_of_call_1", required=False),
        'number_of_call_2': fields.String(description="number_of_call_2", required=False),
        'number_of_call_3': fields.String(description="number_of_call_3", required=False),
    })

    @api.route('/api/v1/biac/kpi101_monthly')
    @api.doc(description="Post kpi101 monthly.",params={'token': 'A valid token'})

    class biacKPI101Monthly(Resource):    
        @token_required()
        @api.doc(description="Post biac kpi 101 monthy.",params={'token': 'A valid token'})
        @api.expect(post_kpi101_monthly)
        def post(self, user=None):
            logger.info("biac - post kpi 101 monthly")
            req= json.loads(request.data.decode("utf-8"))   
            month_to_update = dateutil.parser.parse(req['month_to_update'])

            number_of_call_1 = -1
            number_of_call_2 = -1
            number_of_call_3 = -1

            if 'number_of_call_1' in req:
                number_of_call_1 = req['number_of_call_1']
            if 'number_of_call_2' in req:
                number_of_call_2 = req['number_of_call_2']
            if 'number_of_call_3' in req:
                number_of_call_3 = req['number_of_call_3']

            return {'error':"",'status':'ok', 'data': json.dumps(update_kpi101_monthly(es, month_to_update, number_of_call_1, number_of_call_2, number_of_call_3), cls=DateTimeEncoder)}

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, np.integer): 
            return int(o)
        elif isinstance(o, np.int64): 
            return int(o)
        elif isinstance(o, pd.int64): 
            return int(o)

        return json.JSONEncoder.default(self, o)

##########################################################
#                       KPI101
##########################################################

def update_kpi101_monthly(es, month, number_of_call_1=-1, number_of_call_2=-1, number_of_call_3=-1):
    #logger.info('update_kpi101_monthly date: '+str(month))
    
    start_dt = mkFirstOfMonth(month)
    end_dt   = mkLastOfMonth(month)
    

    logger.info('**'*100)


    logger.info(start_dt)
    logger.info(end_dt)
    
    logger.info(number_of_call_1)
    logger.info(number_of_call_2)
    logger.info(number_of_call_3)
    
    df     = es_helper.elastic_to_dataframe(es,index="biac_kpi101_call*"
                                            ,query='*'
                                            ,start=start_dt
                                            ,end=end_dt
                                            ,timestampfield='datetime')

    df_group = None
    if len(df) > 0:
        df_group = df.groupby('lot').agg({'_id':'count'})

    obj = {
        'not_timely_answer'    : 0,
        'not_timely_answer_1'  : 0,
        'not_timely_answer_2'  : 0,
        'not_timely_answer_3'  : 0,
        'number_of_call_1'     : 0,
        'number_of_call_2'     : 0,
        'number_of_call_3'     : 0,
        'percentage'           : 0,
        'percentage_1'         : 0,
        'percentage_2'         : 0,
        'percentage_3'         : 0,
    }

    try:
        obj['not_timely_answer_1']=df_group.loc[1, '_id']
    except:
        logger.info('no value for 1')
    try:
        obj['not_timely_answer_2']=df_group.loc[2, '_id']
    except:
        logger.info('no value for 2')
    try:
        obj['not_timely_answer_3']=df_group.loc[3, '_id']
    except:
        logger.info('no value for 3')

    df_month = None
    if number_of_call_1 == -1 or number_of_call_2 == -1 or number_of_call_3 == -1:
        df_month     = es_helper.elastic_to_dataframe(es,index="biac_kpi101_monthly*"
                                                        ,query='*'
                                                        ,start=start_dt
                                                        ,end=end_dt
                                                        ,timestampfield='datetime')
        

    if number_of_call_1 == -1:
        try:
            obj['number_of_call_1'] = df_month.iloc[0]['number_of_call_1']
        except:
            obj['number_of_call_1'] = 0
    else:
        obj['number_of_call_1'] = number_of_call_1
        
    if number_of_call_2 == -1:
        try:
            obj['number_of_call_2'] = df_month.iloc[0]['number_of_call_2']
        except:
            obj['number_of_call_2'] = 0
    else:
        obj['number_of_call_2'] = number_of_call_2
        
    if number_of_call_3 == -1:
        try:
            obj['number_of_call_3'] = df_month.iloc[0]['number_of_call_3']
        except:
            obj['number_of_call_3'] = 0
    else:
        obj['number_of_call_3'] = number_of_call_3
        


    obj['not_timely_answer'] = obj['not_timely_answer_1'] + obj['not_timely_answer_2'] + obj['not_timely_answer_3']
    obj['number_of_call']    = obj['number_of_call_1'] + obj['number_of_call_2'] + obj['number_of_call_3']

    if obj['number_of_call'] != 0:
        obj['percentage'] = round(((obj['number_of_call'] - obj['not_timely_answer']) / obj['number_of_call'])*100, 2)
    if obj['number_of_call_1'] != 0:
        obj['percentage_1'] = round(((obj['number_of_call_1'] - obj['not_timely_answer_1']) / obj['number_of_call_1'])*100, 2)
    if obj['number_of_call_2'] != 0:
        obj['percentage_2'] = round(((obj['number_of_call_2'] - obj['not_timely_answer_2']) / obj['number_of_call_2'])*100, 2)
    if obj['number_of_call_3'] != 0:
        obj['percentage_3'] = round(((obj['number_of_call_3'] - obj['not_timely_answer_3']) / obj['number_of_call_3'])*100, 2)
    
    local_timezone = tzlocal.get_localzone()
    
    obj['datetime'] = local_timezone.localize(start_dt)
    _id      = int(obj['datetime'].timestamp())*1000
    
    res = es.index(index="biac_kpi101_monthly", doc_type='doc', id=_id, body=json.dumps(obj, cls=DateTimeEncoder))
    
    logger.info(res)
    
    return obj

##########################################################
#                       KPI304
##########################################################

def get_kpi304_values(es, lot, tec, date):
    query = 'lot:'+lot
    if lot == '2':
        query+=' AND tec:'+tec
        
    print(query)
    containertimezone=pytz.timezone(tzlocal.get_localzone().zone)
        
    start_dt = containertimezone.localize(datetime(date.year, date.month, 1))
    end_dt   = containertimezone.localize(datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 23, 59, 59))
    
    print(start_dt)
    print(end_dt)
    
    dataframe=es_helper.elastic_to_dataframe(es,index="biac_kpi304"
                                            ,datecolumns=["@timestamp"]
                                            ,query=query
                                            ,start=start_dt
                                            ,end=end_dt)
    
    if len(dataframe) == 0:
        print('dataframe empty we create in DB')
        default_df=pd.DataFrame(pd.date_range(start=start_dt, end=end_dt), columns=['_timestamp'])

        if lot == '1' or lot == '2': 
            default_df['tech']=2
            default_df['tech1']=1
            default_df['tech2']=1
        else:
            default_df['tech']=1
        default_df['hoofd']=1

        default_df['dayofweek']=default_df['_timestamp'].dt.dayofweek
        default_df.loc[default_df['dayofweek']>=5, 'tech']=0
        default_df.loc[default_df['dayofweek']>=5, 'hoofd']=0
        default_df['total']=default_df['tech']+default_df['hoofd']

        default_df['tec']=tec
        default_df['lot']=lot

        default_df['_index']='biac_kpi304'
        default_df['_id']=default_df['lot']+'_'+default_df['tec']+'_'+default_df['_timestamp'].astype(str)
        del default_df['dayofweek']

        
        es_helper.dataframe_to_elastic(es, default_df)
        default_df['_timestamp']=default_df['_timestamp'].dt.date.astype(str)

        logger.info('query'*100)
        thr = Timer(5, update_kib_kpi304, (es, lot, tec, date))
        
        thr.start()


        return default_df.rename({'_timestamp': '@timestamp'}, axis='columns').to_json(orient='records')
    
    else:
        dataframe.sort_values('@timestamp', inplace=True)
        dataframe['@timestamp']=dataframe['@timestamp'].dt.date.astype(str)
        return dataframe.to_json(orient='records')


def update_kib_kpi304(es, lot, tec, date):
    containertimezone=pytz.timezone(tzlocal.get_localzone().zone)

    start_dt = containertimezone.localize(datetime(date.year, date.month, 1))
    end_dt   = containertimezone.localize(datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 23, 59, 59))

    query = 'lot:'+str(lot)+' AND tec:'+tec

    logger.info('query'*100)
    logger.info(query)
    logger.info(start_dt)
    logger.info(end_dt)
    logger.info(query)
    df = es_helper.elastic_to_dataframe(es, index='biac_kpi304',datecolumns=["@timestamp"]\
                                            , query=query, start=start_dt, end=end_dt)


    if 'off' not in df:
        df['off'] = 0
    df['off'] = df['off'].fillna(0)
    df['week_day'] = df['@timestamp'].dt.weekday
    logger.info(df.shape)
    df.head()

    new_arr=[]
    for index, row in df.iterrows():
        flag_off = False

        if row['week_day'] == 5 or row['week_day'] == 6 or int(row['off']) == 1:
            flag_off = True

        type_list = ['hoofd', 'tech1', 'tech2']
        if 'tech1' not in row or row['tech1'] != row['tech1']:
            type_list = ['hoofd', 'tech']

        for i in type_list:
            obj = {
                'type': i,
                'lot': row['lot'],
                'kpi304_technic': row['tec'],
                '@timestamp': row['@timestamp'],
            }

            if flag_off:
                obj['value'] = -1
            else:
                obj['value'] = row[i]

            obj['_id']= 'lot'+str(row['lot'])+'_'+row['tec']+'_'+i+'_'+str(int(obj['@timestamp'].timestamp()*1000))

            if obj['type'] == 'hoofd':
                obj['type_nl'] = 'Verantwoordelijke'
            elif obj['type'] == 'tech':
                obj['type_nl'] = 'Technieker'
            elif obj['type'] == 'tech1':
                obj['type_nl'] = 'Technieker 1'
            elif obj['type'] == 'tech2':
                obj['type_nl'] = 'Technieker 2'

            new_arr.append(obj)


    df_to_push=pd.DataFrame(new_arr)

    df_to_push['_index'] = 'biac_kib_kpi304'
    logger.info(df_to_push.shape)

    es_helper.dataframe_to_elastic(es, df_to_push)

##########################################################
#                       KPI104
##########################################################

def update_month_kpi104(es, month):
    logger.info(month)
    
    local_timezone = tzlocal.get_localzone()


    start_dt = month
    end_dt   = datetime(month.year, month.month, calendar.monthrange(month.year, month.month)[1])
    
    logger.info('-------------')
    logger.info(start_dt)
    logger.info(end_dt)
                      
    df     = es_helper.elastic_to_dataframe(es,index="biac_kpi104_check*"
                                            ,query='*'
                                            ,start=start_dt
                                            ,end=end_dt)



    
    logger.info('res len %d' % len(df))
    max_dt = start_dt.astimezone(local_timezone)

    obj_past = None
    try:
        obj_past = es.get(index="biac_kpi104_monthly", doc_type='doc', id=start_dt.astimezone(local_timezone))['_source']
    except elasticsearch.NotFoundError:
        logger.error("Unable to retrive past data")
        logger.error(error)

        obj_past = {
            '@timestamp'     : start_dt.astimezone(local_timezone),
            'last_update'    : start_dt.astimezone(local_timezone),
            'shift_number'   : 0,
            'shift_presence' : 0,
            'percentage'     : 100
        }
        es.index(index="biac_kpi104_monthly", doc_type='doc', id=obj_past['@timestamp'], body=json.dumps(obj_past, cls=DateTimeEncoder))

    logger.info(obj_past)

    shift_presence = 0

    if len(df)==0:
        logger.info('empty data frame')


    else:
        df['dt'] = pd.to_datetime(df['@timestamp'], unit='ms', utc=True)


        shift_number   = 0
        shift_presence   = 0
        percentage = 100
        
        try:
            shift_presence = df[df['value']]['value'].count()
            max_dt = max(df[df['value']]['dt']).to_pydatetime().astimezone(local_timezone)
            shift_number   = max_dt.day * 3
            percentage = 0
        except: 
            logger.info('shift_presence to 0')

            if obj_past['shift_number'] != 0:
                shift_number   = 3
                shift_presence   = 0
                percentage = 0


        logger.info(max_dt)
            
    
    
    logger.info('shift_number   %d ' % shift_number)
    logger.info('shift_presence %d ' % shift_presence)
    
    obj = {
        '@timestamp'     : start_dt.astimezone(local_timezone),
        'last_update'    : max_dt,
        'shift_number'   : shift_number,
        'shift_presence' : shift_presence,
        'percentage'     : percentage
    }
    
    if shift_number != 0:
        obj['percentage'] = round((shift_presence*100)/shift_number, 1)
        
        
    logger.info(json.dumps(obj, cls=DateTimeEncoder))
    
    res = es.index(index="biac_kpi104_monthly", doc_type='doc', id=obj['@timestamp'], body=json.dumps(obj, cls=DateTimeEncoder))
    logger.info(res)

def update_kpi104_monthly(es, date):
    logger.info('update_kpi104_monthly date: '+str(date))
    
    start = date - timedelta(days=date.weekday())
        
    if start.month != date.month:
        month_1             = datetime(start.year, start.month, 1)
        logger.info('update_month -> month_1: %s' %             month_1)
        update_month_kpi104(es, month_1)
        
        month_2             = datetime(date.year, date.month, 1)
        logger.info('update_month -> month_2: %s' %             month_2)
        update_month_kpi104(es, month_2)
    else:
        month             = datetime(date.year, date.month, 1)
        logger.info('update_month -> month: %s'   %             month)
        update_month_kpi104(es, month)

##########################################################
#                       KPI600
##########################################################

def retrieve_kpi_entities_model(es, privileges, kpi='600'):
    entities   = []
    entitiesHT = {}

    res=es.search(index="biac_entity",body={}, size=1000)        
    for rec in res["hits"]["hits"]:
        entities.append(rec["_source"])
        entitiesHT[rec["_source"]["key"]]=rec["_source"]

    return getTechnicsKPIByPriv(entities, privileges, kpi=kpi)

def getTechnicsKPIByPriv(entities, privileges = [], kpi='600'):

    if type(privileges) == str:
        privileges = [privileges]
    logger.info("Get Entities per privileges.["+ ",".join(privileges)+"]")
    
    ret_technics = {}
    
    

    for priv in privileges:
        for rec in entities:
            if 'kpi'+kpi+'_privileges' in rec:                
                for rec_priv in rec['kpi'+kpi+'_privileges']:
                    if priv == 'admin' or rec_priv == priv:
                        if 'kpi'+kpi+'_technics' in rec:
                            if rec['lot'] not in ret_technics:
                                ret_technics[rec['lot']] = []
                            
                            ret_technics[rec['lot']] += rec['kpi'+kpi+'_technics']
            
            elif 'privileges' in rec:
                for rec_priv in rec['privileges']:


                    if priv == 'admin' or  rec_priv == priv:
                        if 'kpi'+kpi+'_technics' in rec:

                            if rec['lot'] not in ret_technics:
                                ret_technics[rec['lot']] = []
                            
                            ret_technics[rec['lot']] += rec['kpi'+kpi+'_technics']

    
    for i in ret_technics:
        ret_technics[i] = list(set(ret_technics[i]))


    logger.info("RESULTS_"*100)
    logger.info(ret_technics)
    return ret_technics

def put_default_values_kpi600_monthly(es, entities, month):
    entities_model = determine_model_600()
    arr = []

    for i in entities_model:
        obj = {
            'lot' : i
        }

        for j in entities_model[i]:
            obj['kpi600_technic'] = j

            arr.append(obj.copy())

    df_kpi600 = pd.DataFrame(arr)
    df_kpi600

    start_dt = mkFirstOfMonth(month)
    local_timezone = tzlocal.get_localzone()

    start_dt = local_timezone.localize(start_dt)

    df_kpi600['@timestamp'] = start_dt
    df_kpi600['kpi601'] = False
    df_kpi600['kpi602'] = False
    df_kpi600['kpi603'] = False
    df_kpi600['cancel_by_customer'] = False
    df_kpi600['_id'] = df_kpi600.apply(lambda row: str(row['lot'])+'_'+
                                       row['kpi600_technic'].replace('/','').replace(' ','').lower()+'_'+
                                       str(int(row['@timestamp'].timestamp()*1000)), axis=1)
    
    bulkbody=''
    for index, row in df_kpi600.iterrows():
        action = {}
        action["index"] = {"_index": 'biac_kpi600_monthly',
            "_type": "doc", "_id": row['_id']}

        try:
            res=es.get(index='biac_kpi600_monthly',doc_type="doc",id= row['_id'])
            logger.info("Record "+row['_id']+ " found. Continuing...")
            continue
        except:
            logger.info("Record "+row['_id']+ " not found. Creating it.... ")            

        obj = {}

        for j in df_kpi600.columns:
            obj[j] = row[j]
        
        if '_id' in obj:
            del obj['_id']

        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += json.dumps(obj, cls=DateTimeEncoder) + "\r\n"

    bulkbody

    bulkres = es.bulk(bulkbody, request_timeout=30)

def get_kpi600_value(es, lot, kpi600_technic, month):
    start_dt = mkFirstOfMonth(month)
    local_timezone = tzlocal.get_localzone()
    start_dt = local_timezone.localize(start_dt)
    
    es_id = (str(lot)+'_'+kpi600_technic+'_'+str(int(start_dt.timestamp()*1000))).lower()
    
    print(es_id)
    
    entities   = []

    res=es.search(index="biac_entity",body={}, size=1000)        
    for rec in res["hits"]["hits"]:
        entities.append(rec["_source"])

    ret = None
    try:
        res = es.get(index='biac_kpi600_monthly', doc_type='doc', id=es_id)
        ret = res['_source']

        logger.info('=='*20)
        logger.info(str(ret))

        local_timezone = tzlocal.get_localzone()
        
    except elasticsearch.NotFoundError:
        print('setting default current month')
        put_default_values_kpi600_monthly(es, entities, month)
        
        ret = {
            'kpi600_technic': kpi600_technic,
            'lot': lot,
            '@timestamp': start_dt.isoformat(),
            'kpi601': True,
            'kpi602': True,
            'kpi603': True,
            'cancel_by_customer': False
        }
        
    ret['_id'] = es_id

    
    next_month = add_months(start_dt, 1)
    next_month_dt = datetime(next_month.year, next_month.month, next_month.day)
    
    es_id = (str(lot)+'_'+kpi600_technic+'_'+str(int(next_month_dt.timestamp()*1000))).lower()
    
    try:
        res = es.get(index='biac_kpi600_monthly', doc_type='doc', id=es_id)
    except elasticsearch.NotFoundError:
        print('setting default next month')
        put_default_values_kpi600_monthly(es, entities, next_month_dt)
    
    print(next_month_dt)
    print(es_id)
    
    
    return ret   



def mkDateTime(dateString,strFormat="%Y-%m-%d"):
    # Expects "YYYY-MM-DD" string
    # returns a datetime object
    eSeconds = time.mktime(time.strptime(dateString,strFormat))
    return datetime.fromtimestamp(eSeconds)

def formatDate(dtDateTime,strFormat="%Y-%m-%d"):
    # format a datetime object as YYYY-MM-DD string and return
    return dtDateTime.strftime(strFormat)

def mkFirstOfMonth2(dtDateTime):
    #what is the first day of the current month
    ddays = int(dtDateTime.strftime("%d"))-1 #days to subtract to get to the 1st
    delta = timedelta(days= ddays)  #create a delta datetime object
    return dtDateTime - delta                #Subtract delta and return

def mkFirstOfMonth(dtDateTime):
    #what is the first day of the current month
    #format the year and month + 01 for the current datetime, then form it back
    #into a datetime object
    return mkDateTime(formatDate(dtDateTime,"%Y-%m-01"))

def mkLastOfMonth(dtDateTime):
    dYear = dtDateTime.strftime("%Y")        #get the year
    dMonth = str(int(dtDateTime.strftime("%m"))%12+1)#get next month, watch rollover
    dDay = "1"                               #first day of next month
    nextMonth = mkDateTime("%s-%s-%s"%(dYear,dMonth,dDay))#make a datetime obj for 1st of next month
    delta = timedelta(seconds=1)    #create a delta of 1 second
    return nextMonth - delta                 #subtract from nextMonth and return

def add_months(sourcedate, months):

    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return date(year, month, day)





def determine_model_304(user=None):

    model = {}

    if user is None or user.get('filters') is None or user.get('filters') == []:
        model = {
            1: ["BACHEA"], 
            2: ["SANI", "ELEC", "FIRE", "HVAC"], 
            3: ["BACEXT"], 
            4: ["BACDNB"]
        }

    else:

        filters = user.get('filters')

        if 'LOT1' in filters:
            model[1] = ['BACHEA']

            
        model[2] = []
        if 'LOT2_BACFIR_GONDELS' in filters:
            model[2].append('FIRE')
        if 'LOT2_BACFIR_ACCESS' in filters:
            model[2].append('FIRE')
        if 'LOT2_BACFIR_FIRE' in filters:
            model[2].append('FIRE')
        if 'LOT2_BACSAN_SANI' in filters:
            model[2].append('SANI')
        if 'LOT2_BACSAN_HVACPA' in filters:
            model[2].append('SANI')
        if 'LOT2_BACELE' in filters:
            model[2].append('ELEC')
        if 'LOT2_BACHVA' in filters:
            model[2].append('HVAC')
        
        
        if 'LOT2_BACSAN' in filters:
            model[2] = ['SANI']
            
        if 'LOT2_BACFIR' in filters:
            model[2] = ['FIRE']
        
        
        if 'LOT2' in filters:
            model[2] = ["SANI", "ELEC", "FIRE", "HVAC"]
            
        model[2] = list(dict.fromkeys(model[2]))
        if model[2] == []:
            del model[2]


        if 'LOT3' in filters:
            model[3] = ['BACEXT']

        if 'LOT4' in filters:
            model[4] = ['BACDNB']


    return model



def determine_model_600(user=None):

    model = {}

    if user is None or user.get('filters') is None or user.get('filters') == []:
        model = {
            1: ['HVAC'], 
            2: ['Gondels', 'Acces', 'Fire', 'Sanitaire', 'HVAC PA/TO', 'Elektriciteit', 'HVAC NT/PB/OT/CON'], 
            3: ['Elektriciteit', 'Fire', 'HVAC', 'Sanitaire/Acces'], 
            4: ['Elektriciteit'],
        }
    else:

        filters = user.get('filters')

        if 'LOT1' in filters:
            model[1] = ['HVAC']

            
        model[2] = []
        if 'LOT2_BACFIR_GONDELS' in filters:
            model[2].append('Gondels')
        if 'LOT2_BACFIR_ACCESS' in filters:
            model[2].append('Acces')
        if 'LOT2_BACFIR_FIRE' in filters:
            model[2].append('Fire')
        if 'LOT2_BACSAN_SANI' in filters:
            model[2].append('Sanitaire')
        if 'LOT2_BACSAN_HVACPA' in filters:
            model[2].append('HVAC PA/TO')
        if 'LOT2_BACELE' in filters:
            model[2].append('Elektriciteit')
        if 'LOT2_BACHVA' in filters:
            model[2].append('HVAC NT/PB/OT/CON')
        
        
        if 'LOT2_BACSAN' in filters:
            model[2] = ['Sanitaire', 'HVAC PA/TO']
            
        if 'LOT2_BACFIR' in filters:
            model[2] = ['Gondels', 'Acces', 'Fire']
        
        
        if 'LOT2' in filters:
            model[2] = ['Gondels', 'Acces', 'Fire', 'Sanitaire', 'HVAC PA/TO', 'Elektriciteit', 'HVAC NT/PB/OT/CON']

        model[2] = list(dict.fromkeys(model[2]))  
        if model[2] == []:
            del model[2]


        if 'LOT3' in filters:
            model[3] = ['Elektriciteit', 'Fire', 'HVAC', 'Sanitaire/Acces']

            if 'filters' in user:
                if "KPI600-HIDE-LOT3HVAC" in user["filters"]:
                    model[3].remove("HVAC")
                if "KPI600-HIDE-LOT3ELEC" in user["filters"]:
                    model[3].remove("Elektriciteit")
                if "KPI600-HIDE-LOT3FIRE" in user["filters"]:
                    model[3].remove("Fire")
                if "KPI600-HIDE-LOT3SANI" in user["filters"]:
                    model[3].remove("Sanitaire/Acces")                    

        if 'LOT4' in filters:
            model[4] = ['Elektriciteit']


    return model