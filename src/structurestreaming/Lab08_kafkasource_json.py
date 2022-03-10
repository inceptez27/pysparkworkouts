from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
"""
PYSPARK_SUBMIT_ARGS --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/hduser/install/mysql-connector-java.jar pyspark-shell
"""

def main():
    spark = SparkSession.builder.appName("Lab03").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    bookschema = spark.read.format("csv") \
                      .option("header",True) \
                      .option("inferschema",True) \
                      .load("file:/home/hduser/stream-data/bookschema.json").schema
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_json1") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 
    
    
   
    df1 =  df.select(from_json(df["value"], bookschema).alias("data"))
    
    #df2 = df1.select("data.title","data.author","data.year_written","data.edition","data.price")
    
    df1.writeStream.format("console").option("truncate",False).start().awaitTermination()
    
    

main()
