from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import * 
from pyspark.sql.functions import udf

con = SparkConf()
sc = SparkContext(conf=con)

sql = SQLContext(sc)

def splitrecord(rec):
	record = rec.split("|")
	return record[0],int(record[1]),record[2],int(record[3])
def ddouble(a):
	return a*a

abc = udf(ddouble, IntegerType())
sql.udf.register("hello",ddouble)

rdd1 = sc.textFile("file:///home/cloudera/Desktop/marks.txt")
records = rdd1.map(splitrecord) 
schema123 = StructType(
	[StructField('name', StringType()),
	 StructField('age',IntegerType()),
	 StructField('sex', StringType()),
	 StructField('marks',IntegerType())])
print (records.collect())
df1 = sql.createDataFrame(records,schema123)
df1.show()
df1.printSchema()

df1.select('name','age', abc('age').alias('age squared')).show()

#This is to use sql
df1.registerTempTable("TestTable")
print ("------------------------------------->")
sql.sql("select name, age , marks, hello(marks) from TestTable").show()
