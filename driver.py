from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType

from src.dboperations import fileops,mysqlops
from src.helper import commons
#changes added
def main():
    
    spark = SparkSession.builder.appName("Lab05").master("local").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    
    df = fileops.readfromfile(spark, "file:/home/hduser/hive/data/custs")
    
    getdis = udf(lambda x: commons.getdiscount(x), IntegerType())
    
    df1 = df.withColumn("discount",getdis(df["age"]))
    
    fnfullname = udf(lambda x,y: commons.getfullname(x, y),StringType())
    
    df2 = df1.withColumn("fullname",fnfullname(df1["fname"],df1["lname"]))
    
    msg = fileops.writeintofile(df2, "file:/home/hduser/custdataset")
    
    print(msg)
    
    mysqldata = mysqlops.readmysqltable(spark, "customer")
    
    mysqldata.show()
    
    
     




main()