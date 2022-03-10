from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import (StructField,StructType,IntegerType,StringType)


def main():
    
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    
    custid = StructField("Custid",IntegerType,True)      
    fname = StructField("Fname",StringType,True)      
    lname = StructField("Lname",StringType,True)      
    age = StructField("Age",IntegerType,True)
    prof = StructField("prof",StringType,True)      
    custschema = StructType([custid,fname,lname,age,prof])
    
    df = spark.readStream \
                  .format("csv") \
                  .schema(custschema) \
                  .option("header", False) \
                  .option("maxFilesPerTrigger", 2) \
                  .option("sep",",") \
                  .load("file:/home/hduser/stream-data")
    
    df1 = df.select("custid","age","prof")
    
    df2 = df1.withColumn("current_dt", current_date())
    
    df2.writeStream.format("console").start().awaitTermination()

main()



"""
 
 Read from Input sources -> Transformation -> Write into Sink 
 
Input Sources:
=============

Rate (for Testing): It will automatically generate data including 2 columns timestamp and value . This is generally used for testing purposes. 
Socket: This data source will listen to the specified socket and ingest any data into Spark Streaming.
File: This will listen to a particular directory as streaming data. It supports file formats like CSV, JSON, ORC, and Parquet.
Kafka: This will read data from Apache Kafka® and is compatible with Kafka broker versions 0.10.0 or higher 


Sink Types:
========== 

Console sink: Displays the content of the DataFrame to console
File sink: Stores the contents of a DataFrame in a file within a directory. Supported file formats are csv, json, orc, and parquet.
Kafka sink: Publishes data to a Kafka topic
Foreachbatch: allows you to specify a function that is executed on the output data of every micro-batch of the streaming query.



"""