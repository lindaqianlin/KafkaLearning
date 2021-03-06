from kafka import KafkaConsumer
import pymongo as pm
import json
import threading

#Connect with MongoDB
myclient = pm.MongoClient('mongodb://localhost:27017/')
mydb = myclient['mydatabase']
myinfo = mydb['myinfo']
myhit = mydb['myhit']

class Consumer(threading.Thread):
    def run(self):
        consumer = KafkaConsumer('test', bootstrap_servers=['127.0.0.1:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        for msg in consumer:
            #step1: read message
            recv = "%s:%d:%d: key=%s value=%s\n" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
            #step2: analyze message
            for name in msg.value.keys():
                curAccount = msg.value[name]['account']
                if name == 'data_info':
                    #Insert One Document into MyInfo
                    myinfo.insert_one(msg.value[name])
                else: #if name.title() == 'data_hit':
                    #Insert One Document into MyHit as real time data
                    myhit.insert_one(msg.value[name])
                    #Embed into MyInfo for historical data analysis
                    cursor = myinfo.find({'account':curAccount})
                    for i in cursor:
                        if 'hitData' in i.keys():
                            myinfo.update_one({'account':curAccount},
                                { '$push': {'hitData':msg.value[name]} })
                        else:
                            hitData=[]
                            hitData.append(msg.value[name])
                            myinfo.update_one(
                                {'account':curAccount},
                                { '$set': {'hitData':hitData} }
                            )
                        break
        consumer.close()

def main():
    threads = [
        Consumer(),
    ]

    for t in threads:
        t.start()

if __name__ == "__main__":
    main()
