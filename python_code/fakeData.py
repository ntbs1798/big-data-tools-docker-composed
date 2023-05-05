import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json

#Initialize kafka producer
pc = KafkaProducer(bootstrap_servers=['localhost:9092'],
                  value_serializer=lambda x:
                  dumps(x).encode('utf-8'))
data = pd.read_csv("./Fitness_trackers.csv")
#Send data to kafka server via topic "demo17"
while True:
    print("Sending data")
    pc.send("demo17", value=data.sample(1).to_dict(orient='records')[0])
    sleep(1)