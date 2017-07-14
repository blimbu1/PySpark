from pyspark import SparkConf,SparkContext

con = SparkConf()
sc = SparkContext(conf=con)

def makerecord(rec):
	record = rec.split("|")
	return record[3]

rdd1 = sc.textFile("file:///home/cloudera/Desktop/Users.txt")
firstline = rdd1.first()
rdd3 = rdd1.filter(lambda z : z<>firstline)
rdd2 = rdd3.map(makerecord)
data = rdd2.countByValue()
for k in data:
	print k,"-", data[k]

