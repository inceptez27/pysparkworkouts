from pyspark.sql import SparkSession
from pyspark.sql.functions import (split)
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
    
    df2 = df1.select(df1["cust"].getItem(0).alias("custid"),df1["cust"].getItem(1).alias("fname"),df1["cust"].getItem(3).alias("age"))
    
    df2.writeStream \
    .foreachBatch(saveToMySql) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
    
def saveToMySql(df,batchid):
    
    #df1 = df.withColumn("Batch", lit(batchid))
    df.write.format("jdbc") \
        .option("url", "jdbc:mysql://localhost/custdb") \
        .option("dbtable", "tblcustomerdata_raw2") \
        .option("user", "root") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("password", "Root123$") \
        .mode("append") \
        .save()
        
    print("written into mysql")
    
    
main()