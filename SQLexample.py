from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.types import *

con = SparkConf()
sc = SparkContext(conf=con)

sql = SQLContext(sc)
rdd1 = sc.textFile("file:///home/cloudera/Desktop/table.txt")
rdd2 = rdd1.map(lambda z: z.split("|"))
df = sql.createDataFrame(rdd2)
print ("-----------------------Specific Field _1 ------------------")
df.select("_1").show()

print ("-----------------------Specific Field _2 ------------------")
df.select(df._2).show()
print  ("------------------------All Data---------------------------")
df.show()
print ("---------------------------Using Alias-----------------")
df.select(df._1.alias("first"),df._2.alias("second")).show()
print("-------------------second, second * 100 ----------------")
df.select(df._2,df._2*100).show()
print("------------------Age over 18 only---------------")
df.filter(df._2>18).show()
print ("-------------------Age over 18 and Female only------")
df.filter((df._2>18)&(df._3=='F')).show()
print("------------------------Ordinary Sort----------------")
df.sort(df._2).show()
print ("------------Sorting with age in asceding order-----------")
df.sort(df._2.asc()).show()
print("--------------Sorting with age in descengin order----------")
df.sort(df._2.desc()).show()
#df1=sql.createDataFrame(records,['name', 'age', 'sex'])
#df1.show()
schema = StructType(
	[StructField('name', StringType(), True),
	 StructField('age', LongType(),True),
	 StructField('sex', StringType(),True)])
df1 = sql.createDataFrame(rdd2, schema)
print("---------------------df1 Show------------------------")
df1.show()
print ("----------------------df1 dtypes----------------------")
print (df1.dtypes)
print("-----------------------------df1.printschema---------------")
df1.printSchema() 
print("-------------------showing name, age-----------------")
df1.select(['name','age']).show()
print ("------------------------showing name , sex--------------")
df1.select(df1.name, df1.age).show()
#print("-----------------------------------All Data---------------------")
#z = df.select("_1","_2").collect()
#print (z)

