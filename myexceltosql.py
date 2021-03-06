#!/usr/bin/env python
from __future__ import print_function
import threading, logging, time
import pandas as pd
import MySQLdb as mdb
from sqlalchemy import create_engine 
from urllib.parse import quote_plus as urlquote
import os
import json
from kafka import KafkaConsumer, KafkaProducer

msg_size = 524288

producer_stop = threading.Event()
consumer_stop = threading.Event()
'''
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers='127.0.0.1:9092')
#df=df.head(1000) #because of time-consuming, only select first 5 lines
for row in df.itertuples():
    print(row)
    print("***********************************")
    jsonStr=json.dumps(row)
    print(jsonStr)
    producer.send('test', jsonStr)
    sleep(0.1)
producer.flush()
'''

class Producer(threading.Thread):
    big_msg = b'1' * msg_size

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.sent = 0

        while not producer_stop.is_set():
            producer.send('test', self.big_msg)
            self.sent += 1
        producer.flush()

class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['my-topic'])
        self.valid = 0
        self.invalid = 0

        for message in consumer:
            if len(message.value) == msg_size:
                self.valid += 1
            else:
                self.invalid += 1

            if consumer_stop.is_set():
                break

        consumer.close()

def init_info(conn, name):
  df=pd.read_excel(name);
  df.to_sql(name='user_info', con=conn, if_exists='replace')

def init_hit(conn, name):
  df=pd.read_excel(name);
  df.to_sql(name='hit', con=conn, if_exists='append')

def main():
    threads = [
        Producer(),
        Consumer()
    ]
    '''Already Done: Convert Excel Files to MySQL table'''
    #password = 'whoamI@123'
    #url='mysql+mysqldb://lqian:%s@localhost:3306/lqiandb?charset=utf8'%urlquote(password)
    #engine = create_engine(url)
    
    #Find all BASF* files and create sql table
    #start_path = './data' # current directory
    #for path,dirs,files in os.walk(start_path):
    #    for filename in files:
    #        name=os.path.join(start_path, filename)
    #        print(name)
    #        if filename == 'user_info.xlsx':
    #            init_info(engine, name)
    #        else:
    #            init_hit(engine, name)
    #engine.dispose()

    #start producer and consumer
    for t in threads:
        t.start()

#    time.sleep(10)
#    producer_stop.set()
#    consumer_stop.set()
#    print('Messages sent: %d' % threads[0].sent)
#    print('Messages recvd: %d' % threads[1].valid)
#    print('Messages invalid: %d' % threads[1].invalid)

if __name__ == "__main__":
    main()
