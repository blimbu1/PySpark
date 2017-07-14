from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf = con)

def check(x):
	if (x > 5):
		return True
	else:
		return False

list1 = [1,2,3,5,1,2,35,13,5,1,12,3,1,12,20]

rdd1=sc.parallelize(list1)
rdd2 = rdd1.filter(check)
data = rdd2.collect()
print (data)

