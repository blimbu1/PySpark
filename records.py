from pyspark import SparkConf, SparkContext
con = SparkConf()
sc = SparkContext(conf = con)

def makerecord(line):
	record = line.split("|")
	return record[0],int(record[2])

def makerecord_info(line):
	rec = line.split("|")
	return rec[0],rec[1],rec[2]

def add(x,y):
	return x+y

def grades(z):
	if z>90:
		return 'A+'
	elif z>80 and z<=90:
		return 'A'
	elif z>70 and z<=80:
		return 'B'
	elif z>60 and z<=70:
		return 'C'
	elif z<=60:
		return 'fail'

rdd1 = sc.textFile("file:///home/cloudera/Desktop/marks.txt")
rdd9 = sc.textFile("file:///home/cloudera/Desktop/personalinfo.txt")
firstline = rdd1.first()
firstline_info = rdd9.first()
rdd2 = rdd1.filter(lambda z: z<>firstline).map(makerecord)
rdd8 = rdd9.filter(lambda y: y<>firstline_info).map(makerecord_info)
rdd3 = rdd2.reduceByKey(add)
counted = rdd2.countByKey()
data1 = rdd3.collect()
#counted = rdd4.collect()
print counted
print data1
data2 = rdd8.collect()
for k in data1:
	print "student id : %s" %k[0]
	for j in data2:
		if k[0]==j[0]:
			print "Name: %s" %j[1]
			print "address: %s" %j[2]
	print "No of subjects: %d" % counted[k[0]]
	print "Total: %d" %k[1]
	print "Percentage: %0.2f" %(float(k[1])/counted[k[0]])
	print "grade: %s" %(grades(float(k[1])/counted[k[0]])) 
	print "--------------------------------------------------------------"

