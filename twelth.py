from pyspark import SparkConf, SparkContext
con = SparkConf()
sc = SparkContext(conf = con)

reading = sc.textFile('file:///home/cloudera/Desktop/trial.txt')
reading1 = reading.map(lambda z : z.encode("ascii", "ignore"))
data1 = reading1.collect()
print data1
