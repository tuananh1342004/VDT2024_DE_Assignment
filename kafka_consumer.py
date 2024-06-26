from kafka import KafkaConsumer
import json
import csv

TOPIC_NAME = 'vdt2024'
BOOTSTRAP_SERVERS = ['localhost:9092', 'localhost:9094']
GROUP_ID = 'kafka-consumer'


consumer = KafkaConsumer(TOPIC_NAME,
                         group_id=GROUP_ID,  
                         bootstrap_servers=BOOTSTRAP_SERVERS, 
                         auto_offset_reset='earliest')

msg_count = 0
for msg in consumer:
    try:
        # msg = msg.decode('utf-8')
        dict = {
            'topic': msg.topic,
            'partition': msg.partition,
            'offset': msg.offset,
            'key': msg.key.decode('utf-8'), 
            'value':msg.value.decode('utf-8')
        }

        print(f'{msg_count}: {dict}')
        msg_count += 1
        
    except KeyboardInterrupt:
        print("Consumer stopped by user")

print(f'topic: {msg.topic}')

consumer.close()