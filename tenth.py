from pyspark import SparkConf, SparkContext
con = SparkConf()
sc = SparkContext(conf = con)

def age(line):
	record = line.split("|")
	return int(record[1])

def maximus(a,b):
	if (a>b):
		return a
	else:
		return b

rdd1 = sc.textFile("file:///home/cloudera/Desktop/Users.txt")
rdd2 = rdd1.map(age)
#data = rdd2.take(10)
max_val = rdd2.reduce(maximus)
#print data
print "The maximum age is: %s" %max_val


