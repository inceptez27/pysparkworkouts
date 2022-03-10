from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

def processdata(rdd,spark):
    if not rdd.isEmpty():
        df = rdd.toDF(["custid","age","prof"])
        #df = spark.createDataFrame(rdd) 
        df.show()
    


def main():
    
    spark = SparkSession.builder.appName("Lab03").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc, 5)
    
    data = ssc.socketTextStream("localhost",9999)

    data1 = data.map(lambda x: x.split(",")) \
            .filter(lambda x: len(x) == 5) \
            .map(lambda x: (int(x[0]),int(x[3]),x[4]))
    
    
    data1.foreachRDD(lambda rdd: processdata(rdd,spark))
    
    ssc.start()
    ssc.awaitTermination()
    
    
    
main()