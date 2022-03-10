from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date


def main():
    
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    df = spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()
    
    df1 = df.withColumn("current_dt", current_date())
    df1.writeStream.format("console").option("truncate", False).start().awaitTermination()
    
    

main()