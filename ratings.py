from pyspark import SparkConf, SparkContext

con = SparkConf()
sc = SparkContext(conf = con)
"""
def movies_list(line):
	rec = line.split("|")
	return rec[0], rec[1]
"""
def ratings_list(line):
	recs = line.split("\t")
	return recs[1],recs[2]

rdd1 = sc.textFile("file:///home/cloudera/Desktop/datas/Movies.item")
rdd9 = sc.textFile("file:///home/cloudera/Desktop/datas/Moving-Ratings-Done.data")

rdd2 = rdd1.map(lambda l: l.split("|"))
rdd8 = rdd9.map(ratings_list)

#print rdd2.take(8)
#print rdd8.collect()

for i in rdd2.collect():
	b = i[1].split(" ")
	if b[0] == "GoldenEye":
		movieid = i[0]
		break
	
count = 0
count_rating = {}
movies_fivestar = {}

for j in rdd8.collect():
	if j[0]==movieid and j[1]=='5':
		count = count + 1

for e in rdd8.collect():
	if e[0]==movieid:
		if e[1] not in count_rating.keys():
			count_rating[e[1]] = 1
		else:
			count_rating[e[1]] = count_rating[e[1]] + 1

for x in rdd8.collect():
	if x[1] == '5':
		if x[0] not in movies_fivestar.keys():
			movies_fivestar[x[0]] = 1
		else:
			movies_fivestar[x[0]] = movies_fivestar[x[0]] + 1 

print count_rating

print "Number of five star ratings: %d" %count

check = 0
for r in movies_fivestar.keys():
	#print r,"=",movies_fivestar[r]
	if movies_fivestar[r] > check:
		check = movies_fivestar[r]
		maxid = r

for m in rdd2.collect():
	if m[0]== maxid:
		print "Movie name is: %s" %m[1]
#print movies_fivestar	

genre = {5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,19:0,20:0,21:0,22:0,23:0}

for d in rdd2.collect():
	for m in range(5,24):
		genre[m] = int(genre[m]) + int(d[m])

print genre
#print rdd2.take(2)	

under_18s = []
rddu = sc.textFile("file:///home/cloudera/Desktop/datas/Users.txt")
rdd_ratings = sc.textFile("file:///home/cloudera/Desktop/datas/Moving-Ratings-Done.data")
rddusers = rddu.map(lambda q:q.split("|"))
rdd_rate = rdd_ratings.map(lambda y:y.split("\t"))
for h in rddusers.collect():
	if int(h[1])<=18:
		under_18s.append(h[0])

movies_under18 = {}
for q in rdd_rate.collect():
	if q[0] in under_18s:
		if q[1] not in movies_under18.keys():
			movies_under18[q[1]] = 1
		else:
			movies_under18[q[1]] = movies_under18[q[1]] + 1
max_mov = 0
for du in movies_under18:
	if movies_under18[du] > max_mov:
		max_mov = movies_under18[du]
		max_ids = du

for mu in rdd2.collect():
	if mu[0] == max_ids:
		print "The movie that under18's rated most is %s"%mu[1]
		print "The number of times it has been rated %d"%max_mov
		break
	
under_18_genre = {5:0,6:0,7:0,8:0,9:0,10:0,11:0,12:0,13:0,14:0,15:0,16:0,17:0,18:0,19:0,20:0,21:0,22:0,23:0}
for mad in rdd2.collect():
	if mad[0] in movies_under18.keys():
		for i in range(5,24):
			under_18_genre[i] =under_18_genre[i]+ int(mad[i]) 
for you in under_18_genre:
	print (you-5),"=",under_18_genre[you]
 					
