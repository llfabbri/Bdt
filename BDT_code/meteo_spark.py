'''
Computations in Spark. Spark fetched the data from Kafka.
Specifically, the original idea was to
put in a dataframe the data,
then modify this dataframe in Spark putting 1 into the
cells containing risk values higher than their threshold,
0 otherwise. But I was not able to modify the Spark df
so I performed the changes on Spark Row objects. The
point is that I believe that this is like using Python
as the output of the Rows modifications are simple tuples.
A way to perform the original idea should be implemented
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType
#from pyspark.sql.streaming import StreamingQuery

class SparkKafkaConsumer: #class to fetch the data from kafka
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers 
        self.kafka_topic = kafka_topic 
        self.spark = self.start_spark_session()

    def start_spark_session(self): #method creates the Spark session and configures it to work with Kafka
        return SparkSession.builder \
            .appName("KafkaWeatherConsumer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \ #configures the Spark session to include the required package for integrating Spark with Kafka. 
        #This package allows Spark to read and write data from/to Kafka topics
            .getOrCreate() #either retrieves an existing Spark session or creates a new one if it doesn't exist.

    def read_from_kafka(self):# method to continuously read data from a specified Kafka topic.
        return self.spark.readStream \
            .format("kafka") \ # specifies that you're reading from a Kafka source, so that partitions are preserved
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load() #initiates the process of loading data from the Kafka topic as a streaming DataFrame

    def process_data(self):
        # Define schema for the DataFrame
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("min_temp", FloatType(), True),
            StructField("max_temp", FloatType(), True),
            StructField("radiations", StringType(), True),
            StructField("wind_kmh", FloatType(), True),
            StructField("precipitazioni", FloatType(), True),
            StructField("hr", FloatType(), True)
        ])

        # Read data from Kafka topic
        kafka_data = self.read_from_kafka()

        # Deserialize the Kafka message value (assuming it's in JSON format)
        kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")

        # Parse JSON and apply schema
        parsed_data = kafka_data.selectExpr("from_json(value, '{}') as data".format(schema.json())).select("data.*")

        # Apply transformations to the data
        modified_data = parsed_data.withColumn("min_temp_modified", expr("IF(min_temp <= 0, 1, 0)"))
        modified_data = modified_data.withColumn("max_temp_modified", expr("IF(max_temp >= 35, 1, 0)"))
        modified_data = modified_data.withColumn("radiations_modified", expr("IF(radiations IN ('high', 'very high'), 1, 0)"))
        modified_data = modified_data.withColumn("wind_kmh_modified", expr("IF(wind_kmh >= 100, 1, 0)"))
        modified_data = modified_data.withColumn("precipitazioni_modified", expr("IF(precipitazioni >= 50, 1, 0)"))
        modified_data = modified_data.withColumn("hr_modified", expr("IF(hr >= 70, 1, 0)"))

        # Select relevant columns for output
        output_data = modified_data.select(
            "city", "min_temp_modified", "max_temp_modified", "radiations_modified",
            "wind_kmh_modified", "precipitazioni_modified", "hr_modified"
        )

        return output_data

    def stop_spark_session(self):
        self.spark.stop()

# Instantiate the SparkKafkaConsumer class
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "weather_data_topic"
consumer = SparkKafkaConsumer(kafka_bootstrap_servers, kafka_topic)

# Process data and get the processed DataFrame
processed_df = consumer.process_data()

# Show the processed DataFrame
processed_df.show()

# Stop the Spark session
consumer.stop_spark_session()

