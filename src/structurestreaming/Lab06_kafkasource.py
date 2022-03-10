from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv
"""
In eclipse IDE, set the following in window -> preference -> pydev -> Python Interpreter -> Environment variable -> Add

PYSPARK_SUBMIT_ARGS --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 pyspark-shell

In spark submit,

spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 Lab06_kafkasource.py

"""

def main():
    spark = SparkSession.builder.appName("Lab03").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    custschema = spark.read.format("csv") \
                      .option("header",True) \
                      .option("inferschema",True) \
                      .load("file:/home/hduser/stream-data/schemadata.csv").schema
                      
    
    print(custschema)
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_topic1") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 
    
    csvoptions = {"delimiter":",","header":"False"}
   
    #df1 =  df.select(from_csv(col=df["value"], schema=custschema.simpleString(),options=csvoptions).alias("data"))
    
    df1 =  df.select(from_csv("value", custschema.simpleString(),csvoptions).alias("data"))
    
    
    df1.writeStream.format("console").start().awaitTermination()
    
    

main()
