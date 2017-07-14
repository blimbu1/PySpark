from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf = con)

list1 = [23,3,234,12,8,10]
rdd1 = sc.parallelize(list1)
data = rdd1.collect()

for a in data:
	print (a)

