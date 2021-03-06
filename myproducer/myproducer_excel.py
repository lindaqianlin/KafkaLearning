#!/usr/bin/env/python
from time import sleep
import os
import json
import pandas as pd
from kafka import KafkaProducer

def main():
    start_path = './data'
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers='127.0.0.1:9092') 

    for path,dirs,files in os.walk(start_path):
        for filename in files:
            name=os.path.join(start_path, filename)
            df = pd.read_excel(name) 
            col_list = df.columns.values.tolist()
            for row in df.itertuples():
                row_dict = {}
                for i in range(len(col_list)):
                    col = col_list[i]
                    row_dict[col] = getattr(row, col, 'NIL')
                if filename=='user_info.xlsx':
                    dict_obj = {'data_info':row_dict}
                else:
                    dict_obj = {'data_hit':row_dict}
                producer.send('test', dict_obj)

            producer.flush()

    producer.close()
if __name__ == "__main__":
    main()
