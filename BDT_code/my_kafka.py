'''
In this script a class to produce a kafka topic and send the data to kafka is created. 
Inside this topic, partitions are made to be equivalent to the number of PAOI we have.
Finally the dictionary of the meteo.py script is sent through this class to kafka
'''
from confluent_kafka import Producer #check your broker kafka version. It must be >= v0.8
import json
from meteo import weather_data_dict

class KafkaWeatherProducer:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

    def send_weather_data(self, topic, key, weather_data):
        try:
            self.producer.produce(topic, key=key.encode('utf-8'), value=json.dumps(weather_data).encode('utf-8'))
            self.producer.flush()
            print(f"Data sent successfully to topic '{topic}' with key '{key}'")
        except Exception as e:
            print(f"Error sending data to topic '{topic}' with key '{key}': {e}")

kafka_bootstrap_servers = 'localhost:9092' #replace based on the local host number you have
kafka_producer = KafkaWeatherProducer(bootstrap_servers=kafka_bootstrap_servers)

topic = 'weather_data_topic'

for city_name, city_data in weather_data_dict.items():
    
    for key in ['min_temp', 'max_temp', 'radiations','wind_kmh','precipitazioni','hr']:
        kafka_producer.send_weather_data(topic, key, city_data[key])

# No need to explicitly close the producer since flush() does it.







