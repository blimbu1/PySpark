from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf = con)

rdd1 = sc.textFile("file:///home/cloudera/Desktop/trainees.txt")
rdd2 = sc.textFile("file:///home/cloudera/Desktop/trainers.txt")

resultrdd1 = rdd1.union(rdd2)
resultrdd2 = rdd1.intersection(rdd2)
resultrdd3 = rdd1.subtract(rdd2)
print resultrdd1.collect()
print resultrdd2.collect()
print resultrdd3.collect()
