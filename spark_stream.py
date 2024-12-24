import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_mongo_connection():
    """
    Create and return a MongoDB client and database connection.
    """
    try:
        client = MongoClient('mongodb', 27017, username='admin', password='adminpassword')
        db = client['spark_streams']
        logging.info("MongoDB connection established successfully")
        return client, db
    except Exception as e:
        logging.error(f"Failed to create MongoDB connection: {e}")
        return None, None

def create_spark_connection():
    """
    Create and return a Spark session configured for Kafka streaming.
    """
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
        return spark_conn
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def connect_to_kafka(spark_conn):
    """
    Connect to Kafka and return the streaming DataFrame.
    """
    try:
        kafka_bootstrap_servers = "broker:29092"
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers ) \
            .option("kafka.request.timeout.ms", "30000") \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka connection established successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    """
    Parse Kafka messages into a structured DataFrame using a predefined schema.
    """
    schema = StructType([
        StructField('id', StringType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('address', StringType(), True),
        StructField('post_code', StringType(), True),
        StructField('email', StringType(), True),
        StructField('username', StringType(), True),
        StructField('registered_date', StringType(), True),
        StructField('phone', StringType(), True),
        StructField('picture', StringType(), True)
    ])

    try:
        sel_df = spark_df.selectExpr('CAST(value AS STRING)') \
            .select(from_json(col('value'), schema).alias('data')) \
            .select('data.*')
        logging.info("Selection DataFrame created successfully")
        return sel_df
    except Exception as e:
        logging.error(f"Failed to create selection DataFrame: {e}")
        return None

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            # Create structured DataFrame from Kafka stream
            selection_df = create_selection_df_from_kafka(spark_df)

            # Create MongoDB connection
            mongo_client, db = create_mongo_connection()

            if db is not None:
                try:
                    # Function to write batch data to MongoDB
                    def write_to_mongo(batch_df, batch_id):
                        try:
                            # Convert DataFrame to JSON and insert into MongoDB
                            records = batch_df.toJSON().collect()
                            documents = [json.loads(record) for record in records]

                            if documents:
                                db.created_users.insert_many(documents)
                                logging.info(f"Batch {batch_id}: Inserted {len(documents)} records into MongoDB")
                            else:
                                logging.warning(f"Batch {batch_id}: No records to insert")
                        except Exception as e:
                            logging.error(f"Batch {batch_id}: Error writing to MongoDB: {e}")

                    # Start streaming query
                    streaming_query = selection_df.writeStream \
                        .foreachBatch(write_to_mongo) \
                        .outputMode("append") \
                        .option("checkpointLocation", "/tmp/checkpoint") \
                        .start()

                    logging.info("Streaming query started successfully")

                    # Await termination of the streaming query
                    streaming_query.awaitTermination()
                except Exception as e:
                    logging.error(f"Streaming process failed: {e}")
                finally:
                    # Close MongoDB connection
                    if mongo_client:
                        mongo_client.close()
                        logging.info("MongoDB connection closed")

            # Stop Spark session
            spark_conn.stop()
            logging.info("Spark session stopped")




