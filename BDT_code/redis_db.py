from meteo import meteo_connector
from meteo_spark import SparkKafkaConsumer
import json
import redis

r = redis.Redis(host='localhost', port=6379, db=0) #check your port

# Create instances
filepath = 'C:/Users/Notebook/Desktop/documentazione/loc_gb.csv'
my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

# Spark dataframe
session = SparkKafkaConsumer()
session.start_session()
rows = session.from_dict_to_rows(diz)
modified_rows = session.modify_rows(rows)
df = session.final_df(modified_rows)

def df_json(my_df):
    '''
    Converts the Spark dataframe
    given as input into a JSON dictionary
    '''
    res = {}
    json_str =  my_df.toJSON().collect() # List of JSON strings
    for my_json in json_str:
        json_dict = json.loads(my_json)
        res[json_dict['city']] = json_dict
    return res

# Store the resulting JSON into Redis
data = df_json(df)

# Here you've been using 'r' to establish the connection to Redis, let's use a different variable name
redis_connection = redis.Redis()

# Storing the JSON data in Redis
redis_connection.set('doc', json.dumps(data))

# Retrieving data for all cities
doc = json.loads(redis_connection.get('doc'))
print(doc)

# Retrieving data for a specific city (e.g., 'Aberdeen')
aberdeen = doc.get('Aberdeen')
print(aberdeen)

