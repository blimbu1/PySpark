from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf = con)

def squared(x):
	return x*x

list2 = [1,2,3,4,5,6,7,8,9]
rdd1 = sc.parallelize(list2)
rdd2 = rdd1.map(squared)
data = rdd2.collect()
for a in data:
	print (a)


