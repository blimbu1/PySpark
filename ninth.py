from pyspark import SparkConf,SparkContext

con = SparkConf()
sc = SparkContext(conf=con)

def makerecord(rec):
	record = rec.split("|")
	return record[0],record[1]

def age(x):
	y = int(x)
	return y+10

rdd1 = sc.textFile("file:///home/cloudera/Desktop/Users.txt")
rdd2 = rdd1.map(makerecord)
#value = rdd2.first()
#print "value = ",
#print value
#print type(value[1])
rdd3 = rdd2.mapValues(age)
for i in rdd3.take(10):
	print i


