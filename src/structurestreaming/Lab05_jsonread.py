from pyspark.sql import SparkSession


def main():
    
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    bookschema = spark.read.format("csv") \
                      .option("header",True) \
                      .option("inferschema",True) \
                      .load("file:/home/hduser/stream-data/bookschema.json").schema
    
    
    df = spark.readStream \
                  .format("json") \
                  .schema(bookschema) \
                  .option("maxFilesPerTrigger", 2) \
                  .load("file:/home/hduser/stream-data")
                  
    df.createOrReplaceTempView("tblbooks")
                  
    df1 = spark.sql("select * from tblbooks where year_written > 1900")
    
    df1.writeStream.format("console").start().awaitTermination()
    
    
main()