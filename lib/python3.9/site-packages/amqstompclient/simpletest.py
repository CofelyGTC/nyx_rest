import amqstompclient
import logging
import time

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

server={"ip":"localhost","port":"61613","login":"admin"
,"password":"******","heartbeats":(10000,10000),"earlyack":True}

def mymessage(destination,message,headers):
    
    for i in range(40):
        print('coucou:'+str(i))
        time.sleep(1)

conn=amqstompclient.AMQClient(server
      , {"name":"TEST","version":"1.0.0","lifesign":"/topic/RPN_MODULE_INFO"}
      ,["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"]
      ,callback=mymessage)        

while True:
    time.sleep(5)
    conn.send_life_sign(variables={"master":1})
