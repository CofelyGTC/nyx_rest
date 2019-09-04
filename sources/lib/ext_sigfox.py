import logging
from flask_restplus import Namespace,Api, Resource, fields
from flask import Flask, jsonify, request
import json


print("***>"*100)

logger=logging.getLogger()

def config(api,conn,es,redis,token_required):
    #---------------------------------------------------------------------------
    # API configRest
    #---------------------------------------------------------------------------

    @api.route('/api/v1/sigfox/data')
    @api.doc(description="Take Sigfox data from Backend Sigfox.",params={'token':'A valid token', 'id':'Sigfox device ID', 'time':'A timestamp', 'Data':'The Sigfox Data', 'Seq':'The seq Number'})
    class sigfox_data(Resource):
        def get(self, user=None):
            logger.info('New Sigfox Data By Get Request')
            token=request.args.get('token')
            if token == 'oZiyUti8W1IPyY2gxCX8VbooT6vwZDA3':
                print(conn)
                device =request.args.get('id')
                timets =request.args.get('time')
                data =request.args.get('data')
                seq =request.args.get('seq')
                mess = {"id": device,
                        "time": timets,
                        "data":data,
                        "seq": seq}
                conn.send_message('/queue/GTC_IMPORT_SIGFOX', json.dumps(mess))
        
        def post(self, user=None):
            logger.info('New Sigfox Data By Post Request')
            req= json.loads(request.data.decode('utf-8'))
            token=req['token']
            if token == 'oZiyUti8W1IPyY2gxCX8VbooT6vwZDA3':
                device =req['id']
                timets =req['time']
                data =req['data']
                seq =req['seq']
                messType=req['messType']
                messSubType=req['messSubType']
                sequence=req['sequence']
                relays=req['relays']
                index1=req['index1']
                index2=req['index2']

                mess = {"id": device,
                        "time": timets,
                        "data":data,
                        "seq": seq,
                        "messType":messType,
                        "messSubType":messSubType,
                        "sequence":sequence,
                        "relays":relays,
                        "index1":index1,
                        "index2":index2}
                conn.send_message('/queue/GTC_IMPORT_SIGFOX', json.dumps(mess))
