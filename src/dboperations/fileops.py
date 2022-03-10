

def readfromfile(spark,path):
    
    df=spark.read.format("csv").option("inferschema",True).load(path).toDF("custid","fname","lname","age","prof")
    
    return df


def writeintofile(df,path):
    
    df.write.mode("overwrite").format("json").save(path)
    
    return "data stored"
    