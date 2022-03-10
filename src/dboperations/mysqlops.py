

def readmysqltable(spark,tablename):
    df = spark.read.format("jdbc") \
     .option("url","jdbc:mysql://localhost/custdb") \
     .option("user","root") \
     .option("password","Root123$") \
     .option("dbtable",tablename) \
     .option("driver","com.mysql.cj.jdbc.Driver") \
     .load()
     
    return df