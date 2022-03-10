from pyspark.sql import SparkSession

"""
PYSPARK_SUBMIT_ARGS --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/hduser/install/mysql-connector-java.jar pyspark-shell
"""

def main():
    spark = SparkSession.builder.appName("Lab03").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .option("group.id", "grptest") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    
    df.writeStream.format("console").start().awaitTermination()
    


main()
