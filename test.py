b = [('1',2),('1',3),('l',8),('l',9),('l',10),('a',5)]

empt_dict = {}
for j in b:
	if j[0] not in empt_dict.keys():
		empt_dict[j[0]] = 1
	else:
		empt_dict[j[0]] = empt_dict[j[0]] + 1

print empt_dict
	
