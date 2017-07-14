from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf=con)

def breakrecord(rec):
	record = rec.split("|")
	if record[2]=='M':
		return True
	else:
		return False


rdd1=sc.textFile("file:///home/cloudera/Desktop/Users.txt")
rdd2 = rdd1.filter(breakrecord)
#data = rdd2.collect()
val = rdd2.count()
print (val)

