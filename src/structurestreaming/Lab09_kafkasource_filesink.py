from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv
"""
PYSPARK_SUBMIT_ARGS --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /home/hduser/install/mysql-connector-java.jar pyspark-shell
"""

def main():
    spark = SparkSession.builder.appName("Lab03").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    custschema = spark.read.format("csv") \
                      .option("header",True) \
                      .option("inferschema",True) \
                      .load("file:/home/hduser/stream-data/schemadata.csv") \
                      .schema
    
    print(custschema)
    df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .option("startingOffsets","""{"customer_topic":{"0":50}}""") \
    .load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 
    
    csvoptions = {"delimiter":",","header":"False"}
   
    df1 =  df.select(from_csv(df["value"], custschema.simpleString(),csvoptions).alias("cust"))
    
    df2 = df1.select("cust.custid","cust.firstname","cust.lastname","cust.age","cust.profession")
    
    #df1.writeStream.format("console").start().awaitTermination()
    
    df2.writeStream.format("csv") \
    .option("path", "file:/home/hduser/sparkcuststreamout1") \
    .option("checkpointLocation", "file:/tmp/sparkchkpoint1") \
    .start() \
    .awaitTermination()
    
    

main()



"""

option("startingoffsets", "latest")  - wait only for the new messages in the topic. 

option("startingoffsets", "earliest")  - allows rewind for missed alerts.  

{"topicA":{"0":23,"1":-1},"topicB":{"0":-1}}

 (-1 is used for the 'latest', -2 - for the earliest) 


"""
