import boto3
import json
from time import time
from random import seed
from random import random

message_count = 500 #total number of messages to send, accross all batches
channel_name = 'iotanium-datavis'
messages_per_batch = 10

client = boto3.client('iotanalytics')
messages = []
message = {}

def rnd_walk(initial_value, increment_val, decrement_val, min, max):
    delta = -decrement_val if random() < 0.5 else increment_val
    new_val = initial_value + delta
    if new_val > max:
        return max
    if new_val < min:
        return min
    else:
        return new_val

temp_celsius=0
battery_lvl=100
geo_lat=44.968046
geo_long=-94.420307

now = int(time())
index = 0
for i in range(now-message_count,now):
    temp_celsius = rnd_walk(temp_celsius, .26, .25, -15, 100)
    battery_lvl = rnd_walk(battery_lvl, 0, .01 , 0, 100)
    geo_lat = rnd_walk(geo_lat, .000001, .000001, -1000, 1000)
    geo_long = rnd_walk(geo_long, .000001, .000001, -1000, 1000)
    message['unix_timestamp'] = i
    message['temp_celsius'] = temp_celsius
    message['battery_lvl'] = battery_lvl
    message['geo_lat'] = geo_lat
    message['geo_long'] = geo_long
    data = json.dumps(message)
    messages.append({
        'messageId': "msg-%s" % str(i), 
        'payload': data
    })
    print("messageId: %s" % str(i))
    print("   payload: %s" % data)
    index += 1

    if i%10 == 0:
        res = client.batch_put_message(
            channelName = channel_name,
            messages = messages
        )
        messages=[]
        print("      sent %s messages, HTTPStatusCode: %s" % (messages_per_batch, res['ResponseMetadata']['HTTPStatusCode']))