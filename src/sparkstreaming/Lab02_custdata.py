
from pyspark import SparkContext
from pyspark.streaming import StreamingContext 


def main():
    
    sc = SparkContext(appName="Lab01-stream",master="local")
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc,5)
    
    dstrm = ssc.textFileStream("file:/home/hduser/stream-data")
    
    dstrm1 = dstrm.map(lambda x : x.split(","))
    
    dstrm2 = dstrm1.filter(lambda x : len(x) == 5)
    
    dstrm3 = dstrm2.map(lambda x : (x[0],x[3],x[4]))
    
    dstrm3.pprint()    
    
    ssc.start()
    ssc.awaitTermination()
    
    
    
main()   