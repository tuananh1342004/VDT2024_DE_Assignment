import csv
import json
import logging

from kafka import KafkaProducer


TOPIC_NAMES = ['vdt2024']
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094"]
data_paths = ["data/log_action.csv"]


def format_data(data):
    json_dict = {}
    json_dict['student_code'] = int(data[0])
    json_dict['activity'] = str(data[1])
    json_dict['numberOfFile'] = int(data[2])
    json_dict['timestamp'] = str(data[3])
        
    return json_dict


def getKey(data):
    return 'even' if (int(data[0])%2 == 0) else 'odd' 
    

def streamData():
    # Init producer    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode('utf-8'),
        value_serializer=lambda value: value.encode('utf-8'),
    )
    
    
    # Read data from CSV and send to Kafka
    record_count = 0
    with open(data_paths[0], mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            try:
                key = getKey(row)
                data = format_data(row)
                # data = ','.join(row)
                
                
                # print(data)
                producer.send(TOPIC_NAMES[0], key=key, value=json.dumps(data))
                # producer.send(TOPIC_NAMES[0], key=key, value=data)
                
                
                record_count += 1
            except Exception as exc:
                logging.error(f'An error occured: {exc}')
                continue
            
        print(f'{record_count} records sent to topic \'{TOPIC_NAMES[0]}\' of Kafka successfully')
    




# MAIN FUNCTION
if __name__ == "__main__":
    streamData()