from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf=con)

list3 = [1,2,3,5,6,7,9,8,1,2,3,1,2,7,8,9,2,3,4,8,9,10,1,7,8,2,7,9,9]
rdd1 = sc.parallelize(list3)
num = rdd1.count()
data = rdd1.countByValue()
print data
for k in data:
	print (k, "-",data[k])



