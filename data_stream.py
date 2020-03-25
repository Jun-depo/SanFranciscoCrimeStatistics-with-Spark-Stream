import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark.sql.window import Window


# TODO Create a schema for incoming resources
schema = StructType([
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True)                
        ])


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", "sf.crimes") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 20) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*") \
        
       

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("call_date_time","original_crime_type_name", "disposition")
        
    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_date_time", "30 minutes") \
        .groupBy("original_crime_type_name", psf.window(distinct_table.call_date_time, "60 minutes", "30 minutes")) \
        .count().sort("count", ascending=False)

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream

    query = agg_df \
        .selectExpr("CAST(original_crime_type_name AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("complete") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "sf.crimes.counts.1hour_window") \
        .trigger(processingTime="5 SECONDS") \
        .option("truncate", "false") \
        .option("checkpointLocation", "sf_crime_checkpoint/") \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, radio_code_df.disposition == agg_df.disposition)

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config('spark.executor.memory', '1g') \
        .config('spark.driver.memory', '1g') \
        .getOrCreate()


    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
