from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('logregconsult').getOrCreate()
data = spark.read.csv('file:/home/hduser/customer_churn.csv',inferSchema=True,
                     header=True)

data.printSchema()

data.describe().show()

data.columns

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=['Age',
 'Total_Purchase',
 'Account_Manager',
 'Years',
 'Num_Sites'],outputCol='features')

output = assembler.transform(data)

final_data = output.select('features','churn')

train_churn,test_churn = final_data.randomSplit([0.7,0.3])

from pyspark.ml.classification import LogisticRegression

lr_churn = LogisticRegression(labelCol='churn')

fitted_churn_model = lr_churn.fit(train_churn)

training_sum = fitted_churn_model.summary

training_sum.predictions.describe().show()

#Evaluation

from pyspark.ml.evaluation import BinaryClassificationEvaluator

"""
ROC is a probability curve and AUC represents the degree or measure of separability. 
It tells how much the model is capable of distinguishing between classes. 
Higher the AUC, the better the model is at predicting 0 classes as 0 and 1 classes as 1.
"""

pred_and_labels = fitted_churn_model.evaluate(test_churn)

pred_and_labels.predictions.show()


churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',
                                           labelCol='churn')

auc = churn_eval.evaluate(pred_and_labels.predictions)

print(auc)

#predict on brand new unlabled data

final_lr_model = lr_churn.fit(final_data)

new_customers = spark.read.csv('file:/home/hduser/new_customers.csv',inferSchema=True,
                              header=True)

new_customers.printSchema()

test_new_customers = assembler.transform(new_customers)

test_new_customers.printSchema()

final_results = final_lr_model.transform(test_new_customers)

final_results.select('Company','prediction').show()

