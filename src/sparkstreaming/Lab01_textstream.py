from pyspark import SparkContext
from pyspark.streaming import StreamingContext 


def main():
    
    sc = SparkContext(appName="Lab01-stream",master="local")
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc,5)
    
    dstrm = ssc.textFileStream("file:/home/hduser/stream-data")
    
    dstrm.pprint()    
    
    ssc.start()
    ssc.awaitTermination()
    
    
    
main()   