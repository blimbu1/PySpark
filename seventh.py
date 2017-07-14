from pyspark import SparkConf,SparkContext

con = SparkConf()
sc = SparkContext(conf=con)

def makerecord(rec):
	record = rec.split("|")
	return record[3]

rdd1 = sc.textFile("file:///home/cloudera/Desktop/Users.txt")
rdd2 = rdd1.map(makerecord)
data = rdd2.countByValue()
for k in data:
	print k,"-", data[k]

