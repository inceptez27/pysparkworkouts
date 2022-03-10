
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="Lab01-stream")
ssc = StreamingContext(sc, 5)

data = ssc.textFileStream("file:/home/hduser/datastream")

data.pprint()

ssc.start()
ssc.awaitTermination()