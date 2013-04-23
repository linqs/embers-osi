#!/usr/bin/python

from haversine import points2distance
from heapq import nlargest
from TwoDTree import TwoDTree 

def distance(x1, y1, x2, y2):
    start = ((x1, 0, 0), (y1, 0, 0))
    end = ((x2, 0, 0), (y2, 0, 0))  
    return points2distance(start, end)

k = 3

gazatteer = open('../aux_data/wg-partial.orig.txt','r')

longitudes = dict()
latitudes = dict()
names = dict()

for line in gazatteer:
	tokens = line.strip().split("\t")
	idNum = tokens[0]
	name = tokens[1]
	latitude = float(tokens[6]) / 100
	longitude = float(tokens[7]) / 100

	names[idNum] = name
	longitudes[idNum] = longitude
	latitudes[idNum] = latitude 

	#print "%s is at (%f, %f)" % (name, latitude, longitude)

print "lat in [%f,%f], long in [%f,%f]" % (min(latitudes.values()),max(latitudes.values()),min(longitudes.values()),max(longitudes.values()))

tree = TwoDTree()
tree.set_geodesic(True)
   
# does naive kNN
output = open("../aux_data/neighbors10.txt", 'w')

for idNum in names.keys():
	tree.insert(latitudes[idNum], longitudes[idNum], idNum)
	# print "%s: (%f,%f)" % (names[idNum], latitudes[idNum], longitudes[idNum])

tree = tree.balance()


for idNum in names.keys():
	x = latitudes[idNum]
	y = longitudes[idNum]
	(neighbors, (nx, ny)) = tree.knn(x, y, k+1)
	
	#distances = [distance(latitudes[idNum], longitudes[idNum], latitudes[i], longitudes[i]) for i in names.keys()]

	#print "Average distance %f" % (float(sum(distances)) / float(len(distances)))

	for n in neighbors:
		if n != idNum:
			output.write("%s\t%s\n" % (idNum,n))

	#print "Neighbors for %s:" % names[idNum]
	#print [names[i] for i in neighbors]
	print [distance(latitudes[idNum], longitudes[idNum], latitudes[i], longitudes[i]) for i in neighbors]

output.close()

