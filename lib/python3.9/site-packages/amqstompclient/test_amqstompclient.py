import amqstompclient
import unittest
import logging
import time

#git tag 1.0.1 -m "PyPi tag"
#git push --tags origin master
#python setup.py sdist
#twine upload dist/*

server={"ip":"localhost","port":"61613","login":"admin","password":"*******"}

logger=None

class TestClient(unittest.TestCase):
    """
    Test amq stomp client
    """
   

    def test_sendreceive(self):
        """
        Send Receive
        """
        logger.info("==> test_sendreceive")
        try:            
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"])        
            conn.send_message("/queue/QTEST1","TEST1_MYMESSAGE1")
            conn.send_message("/queue/QTEST2","TEST1_MYMESSAGE2")
            conn.send_message("/topic/TTEST1","TEST1_MYMESSAGE3")
            conn.send_message("/topic/TTEST2","TEST1_MYMESSAGE4")
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            logger.info(conn.listener.received)
            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)
            self.assertEqual("/queue/QTEST2" in conn.listener.received, True)
            self.assertEqual("/topic/TTEST1" in conn.listener.received, True)
            self.assertEqual("/topic/TTEST2" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 1)
            self.assertEqual(conn.listener.received["/queue/QTEST2"], 1)
            self.assertEqual(conn.listener.received["/topic/TTEST1"], 1)
            self.assertEqual(conn.listener.received["/topic/TTEST2"], 1)

            self.assertEqual(conn.listener.globalerrors, 0)
        finally:
            conn.disconnect()

    def test_sendreceivequeue(self):
        """
        Send Receive Queue
        """
        try:
            logger.info("==> test_sendreceivequeue")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1"])        
            for i in range(0,10):
                conn.send_message("/queue/QTEST1","TEST2_MYMESSAGE_"+str(i))
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)

        finally:    
            conn.disconnect()

    def test_sendreceivetopic(self):
        """
        Send Receive Topic
        """
        try:
            logger.info("==> test_sendreceivetopic")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/topic/TTEST1"])        
            for i in range(0,10):
                conn.send_message("/topic/TTEST1","TEST3_MYMESSAGE_"+str(i))
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            self.assertEqual("/topic/TTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/topic/TTEST1"], 10)

        finally:    
            conn.disconnect()

    def callback1(self, destination, message,headers):
        self.assertEqual(message, "TEST_MYMESSAGECALLBACK")

    def test_callback_queue(self):
        """
        Callback Queue
        """
        try:
            logger.info("==> test_callback")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1"],callback=self.callback1)        

            for i in range(0,10):
                conn.send_message("/queue/QTEST1","TEST_MYMESSAGECALLBACK")
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)
    
        finally:
            conn.disconnect()
    
    def callback2(self, destination, message,headers):
        self.assertEqual(message, "TEST_MYMESSAGECALLBACK")

    def test_callback_topic(self):
        """
        Callback Topic
        """
        try:
            logger.info("==> test_callback")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/topic/TTEST1"],callback=self.callback2)        

            for i in range(0,10):
                conn.send_message("/topic/TTEST1","TEST_MYMESSAGECALLBACK")
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            self.assertEqual("/topic/TTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/topic/TTEST1"], 10)
        finally:
            conn.disconnect()

    def test_sendreceivevolume(self):
        """
        Volume
        """
        try:

            logger.info("==> test_sendreceivevolume")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"])        
            for i in range(0,10):
                conn.send_message("/queue/QTEST1","MYMESSAGE_"+str(i))
            for i in range(0,20):
                conn.send_message("/queue/QTEST2","MYMESSAGE_"+str(i))
            for i in range(0,30):
                conn.send_message("/topic/TTEST1","MYMESSAGE_"+str(i))
            for i in range(0,40):
                conn.send_message("/topic/TTEST2","MYMESSAGE_"+str(i))
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            logger.info(conn.listener.received)
            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)
            self.assertEqual("/queue/QTEST2" in conn.listener.received, True)
            self.assertEqual("/topic/TTEST1" in conn.listener.received, True)
            self.assertEqual("/topic/TTEST2" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)
            self.assertEqual(conn.listener.received["/queue/QTEST2"], 20)
            self.assertEqual(conn.listener.received["/topic/TTEST1"], 30)
            self.assertEqual(conn.listener.received["/topic/TTEST2"], 40)        

        finally:
            conn.disconnect()

    def callback3(self, destination, message,headers):
        self.assertEqual(len(message), 10000)

    def test_sendreceivesize(self):
        """
        Message Size
        """
        try:

            logger.info("==> test_sendreceivesize")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"],callback=self.callback3)        
            for i in range(0,10):
                conn.send_message("/queue/QTEST1","0123456789"*1000)
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            logger.info(conn.listener.received)
            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)

        finally:
            conn.disconnect()
    
    def callback4(self, destination, message,headers):
        self.assertEqual(len(message), 100)
        if headers == None:
            raise ValueError("No headers defined.")
        if headers["myvalue1"]!= "100":
            raise ValueError("Myvalue 1 not set.")
        if headers["myvalue2"]!= "petitlapin":
            raise ValueError("Myvalue 2 not set.")


    def test_sendreceiveheaders(self):
        """
        Headers
        """
        try:

            logger.info("==> test_sendreceiveheaders")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1","/queue/QTEST2","/topic/TTEST1","/topic/TTEST2"],callback=self.callback4)        
            for i in range(0,10):
                conn.send_message("/queue/QTEST1","0123456789"*10,headers={"myvalue1":100,"myvalue2":"petitlapin"})
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            logger.info(conn.listener.received)
            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)
            self.assertEqual(conn.listener.globalerrors, 0)
        finally:
            conn.disconnect()

    def callback5(self, destination, message,headers):
        raise ValueError("An Error")

    def test_callback_error(self):
        """
        Callback Queue
        """
        try:
            logger.info("==> test_callback_error")
            conn=amqstompclient.AMQClient(server, {"name":"TEST","version":"1.0.0"},["/queue/QTEST1"],callback=self.callback5)        

            for i in range(0,10):
                conn.send_message("/queue/QTEST1","TEST_MYMESSAGECALLBACK")
            
            logger.info("==> Waiting for reception")
            time.sleep(2)

            self.assertEqual("/queue/QTEST1" in conn.listener.received, True)

            self.assertEqual(conn.listener.received["/queue/QTEST1"], 10)
            self.assertEqual(conn.listener.globalerrors, 10)
    
        finally:
            conn.disconnect()
    
    def test_multiclient(self):
        """
        Callback Queue
        """
        try:
            logger.info("==> test_multiclient")
            conn1=amqstompclient.AMQClient(server, {"name":"TEST1","version":"1.0.0"},["/queue/QTEST1","/topic/TTEST1"])        
            conn2=amqstompclient.AMQClient(server, {"name":"TEST2","version":"1.0.0"},["/queue/QTEST2","/topic/TTEST1"])        

            for i in range(0,10):
                conn1.send_message("/queue/QTEST2","TEST_TOCLIENT2")
            for i in range(0,5):
                conn2.send_message("/queue/QTEST1","TEST_TOCLIENT1")

            for i in range(0,10):
                conn1.send_message("/topic/TTEST1","TEST_TOBOTH")
                conn2.send_message("/topic/TTEST1","TEST_TOBOTH")

            
            for i in range(0,6):
                logger.info("==> Waiting for reception")
                time.sleep(1)

            self.assertEqual(conn1.listener.received["/queue/QTEST1"], 5)
            self.assertEqual(conn2.listener.received["/queue/QTEST2"], 10)
            self.assertEqual(conn1.listener.received["/topic/TTEST1"], 20)
            self.assertEqual(conn2.listener.received["/topic/TTEST1"], 20)

            self.assertEqual(conn1.listener.globalerrors, 0)
            self.assertEqual(conn2.listener.globalerrors, 0)
    
        finally:
            conn1.disconnect()
            conn2.disconnect()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger()
    logger.info("Starting tests")

    unittest.main()
