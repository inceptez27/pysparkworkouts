from pyspark.sql import SparkSession
from pyspark.sql.functions import (split,to_json)
"""
PYSPARK_SUBMIT_ARGS --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/hduser/install/mysql-connector-java.jar pyspark-shell
"""

def main():
    spark = SparkSession.builder.appName("Lab03").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customertopic1") \
    .option("startingOffsets","latest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    df1 =  df.select(split(df["value"],",").alias("cust"))
    
    
    df3 = df1.withColumn("value", to_json(df1["cust"]))
        
    
    
    df3.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "customerkafkatopic") \
    .option("checkpointLocation", "file:/tmp/sparkkafkasink1") \
    .start() \
    .awaitTermination()
    
    
main()    
    