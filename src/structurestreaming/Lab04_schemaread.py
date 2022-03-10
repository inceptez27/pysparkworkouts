from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date



def main():
    
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    custschema = spark.read.format("csv") \
                      .option("header",True) \
                      .option("inferschema",True) \
                      .load("file:/home/hduser/stream-data/schemadata.csv").schema
    
    
    df = spark.readStream \
                  .format("csv") \
                  .schema(custschema) \
                  .option("header", False) \
                  .option("maxFilesPerTrigger", 2) \
                  .load("file:/home/hduser/stream-data")
    
    df1 = df.select("custid","age","profession")
    
    df2 = df1.withColumn("current_dt", current_date())
    
    df2.writeStream.format("console").start().awaitTermination()
    
    
main()