{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## San Francisco Crime Statistics with Spark Stream\n",
    "\n",
    "The San Francisco Crime data was (or are being) collected as a json log file. The goal of project was to use Spark Structured Stream, process data as live stream and then export an aggregate new stream that can be used for  live analytics.  I used Kafka producer to produce streaming data.  Instead of using Kafka consumer to process data, I used Spark to consume and process data because Spark RDD based structure that is fault tolerance, distributed and partitioned.  Spark also comes with SQL and streaming functionalities to process and aggregate streaming data.  The resulting aggregated stream can provide insights from the data.  In this case, we aggregate data by counting number of crimes in different categories of crimes.  \n",
    "\n",
    "<img align = \"left\" src=\"./images/image4.png\" style=\"width:300px;height:420px;\">"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Dataset:\n",
    "\n",
    "(1) police-department-calls-for-service.json\n",
    "(2) radio_code.json\n",
    "\n",
    "\n",
    "### Requirement:\n",
    "\n",
    "(1) requirements.txt\n",
    "\n",
    "(2) Install requirement\n",
    "run ./start.sh at terminal or pip install -r requirements.txt\n",
    "\n",
    "### Development Environment:\n",
    "I developed the project locally as described below:\n",
    "    • Spark 2.4.5 \n",
    "      (version 2.4.3 and kafka_2.11_2.3.0 gave me error with trigger.processingTime=20 or “20 SECONDS”, that I couldn’t resolve it (could be due my local environment with other programs installed))\n",
    "    • Scala 2.12 (instead of 2.11)\n",
    "    • Java 1.8.x\n",
    "    • Kafka build with Scala 2.12 version 2.3\n",
    "    • Python 3.7.4\n",
    "\n",
    "### Python code files:\n",
    "\n",
    "(1) producer_server.py\n",
    "\n",
    "KafkaProducer can be used to send message to a topic. \n",
    "\n",
    "(2) kafka_server.py\n",
    "Use KafkaProducer to produce data into a Kafka topic (“sf.crimes”) from the data source (police-department-calls-for-service.json).\n",
    "\n",
    "(3) data_stream.py\n",
    "Use Spark as stream consumer to ingest data stream from Kafka topic (“sf.crimes”), then export intermediate Spark stream and aggregate data to produce stream (with writestream) to kafka data sink.\n",
    "\n",
    "(4) consumer_server.py\n",
    "Use confluent_kafka Consumer to check if messages were produced into “sf.crimes” topic by the KafkaProducer.\n",
    "\n",
    "### Run the code:\n",
    "\n",
    "(1) Running zookeeper:\n",
    "\n",
    "./bin/zookeeper-server-start.sh config/zookeeper.properties\n",
    "\n",
    "(2) Running kafka-server: \n",
    "\n",
    "./bin/kafka-server-start.sh config/server.properties\n",
    "\n",
    "(3) Produce messages to kafka-topic:\n",
    "\n",
    "python kafka_server.py\n",
    "\n",
    "(4) Use Kafka consumer to check messages in the topic (topic: sf.crimes):\n",
    "\n",
    "./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sf.crimes --from-beginning\n",
    "\n",
    "The follow screenshot shows messages were produced into the topic.\n",
    "\n",
    "<img src=\"./images/image1.png\" style=\"width:700px;height:340px;\">\n",
    "\n",
    "(5) Run Spark stream:\n",
    "\n",
    "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local[*] path_to data_stream.py\n",
    "\n",
    "<img src=\"./images/console.png\" style=\"width:500px;height:740px;\">\n",
    "\n",
    "(6) Spark Web UI:\n",
    "\n",
    "IP-address:4040\n",
    "4040 is the default port.  It can be configured to use other ports.  \n",
    "\n",
    "<img src=\"./images/job2.png\" style=\"width:1000px;height:220px;\">"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?\n",
    "Increasing “maxOffsetsPerTrigger” can increase “processedRowsPerSecond”.\n",
    "\n",
    "Decreasing trigger.processingTime can decrease latency between “Executor driver added” and start of execution (see below)."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?\n",
    "\n",
    "#### (1) Changing “maxOffsetsPerTrigger” seems to efficient SparkSession property\n",
    "      \n",
    "with maxRatePerPartition = 20\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 200, “processedRowsPerSecond” is around 130\n",
    "\n",
    "<img src=\"./images/image2.png\" style=\"width:500px;height:220px;\">\n",
    "\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 400, “processedRowsPerSecond” is around 230\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 600, “processedRowsPerSecond” is around 330\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 800, “processedRowsPerSecond” is around 500\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 1600, “processedRowsPerSecond” is around 900\n",
    "\n",
    "<img src=\"./images/image3.png\" style=\"width:500px;height:220px;\">\n",
    "\n",
    "* At “maxOffsetsPerTrigger” = 5000, “processedRowsPerSecond” is around 900\n",
    "\n",
    "Therefore “maxOffsetsPerTrigger” = 1600 is close to be optimized.\n",
    "      \n",
    "#### (2) Another efficient SparkSession property is trigger.processingTime \n",
    "trigger.processingTime = 5 seconds:\n",
    "From “Executor driver added” to \"start of execution\" is also around 30 seconds\n",
    "\n",
    "<img src=\"./images/plot1.png\" style=\"width:1000px;height:220px;\">\n",
    "\n",
    "trigger.processingTime = 2 seconds:\n",
    "From “Executor driver added” to \"start of execution\" is around 16-17 seconds\n",
    "\n",
    "\n",
    "<img src=\"./images/plot2.png\" style=\"width:1000px;height:220px;\">\n",
    "\n",
    "* Therefore, lower trigger.processingTime decreases latency of the execution.\n",
    "\n",
    "* trigger.processingTime = 1 second:\n",
    "\n",
    "* From “Executor driver added” to \"start of execution\" is around 15.5 seconds\n",
    "\n",
    "* Trigger.processingTime = 1 second or 2 seconds got very close results.  Therefore , the optimum is around 1 to 2 seconds.  I set  maxOffsetsPerTrigger” = 200 in these experiments."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "base",
   "language": "python",
   "name": "base"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}