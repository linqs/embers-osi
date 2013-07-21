#!/usr/bin/python

import re
import codecs
import unicodedata

minPop = 1

num = 0
trim = 0

names = set()

fout = codecs.open("wg-full-parsed.txt", "w", encoding="utf-8")

name2id = dict()
id2pop = dict()

for line in codecs.open("wg-full.txt", "r", encoding = "utf-8"):

	tokens = line.strip().split('\t')
	
	uniqueID = ''
	name = ''
	altNames = ''
	origNames = ''
	geoType = ''
	population = ''
	latitude = ''
	longitude = ''
	country = ''
	admin1 = ''
	admin2 = ''
	admin3 = ''

	if len(tokens) >= 1:
		uniqueID = tokens[0]
	if len(tokens) >= 2:
		name = tokens[1]
	if len(tokens) >= 3:
		altNames = tokens[2]
	if len(tokens) >= 4:
		origNames = tokens[3]
	if len(tokens) >= 5:
		geoType = tokens[4]
	if len(tokens) >= 6:
		population = tokens[5]
	if len(tokens) >= 7:
		latitude = tokens[6]
	if len(tokens) >= 8:
		longitude = tokens[7]
	if len(tokens) >= 9:
		country = tokens[8]
	if len(tokens) >= 10:
		admin1 = tokens[9]
	if len(tokens) >= 11:
		admin2 = tokens[10]
	if len(tokens) >= 12:
		admin3 = tokens[11]

	# only use localities with population more than minPop

	if geoType != "locality" or float(population) < minPop:
		continue

	if int(uniqueID) < 0:
		continue

	# print "id %s, name %s, altNames %s, geoType %s, population %s, lat %s, long %s, country %s, admin1 %s, admin2 %s, admin3 %s" % \
	# 	(uniqueID, name, altNames, geoType, population, latitude, longitude, country, admin1, admin2, admin3)

	names.add(name)

	# check for exceptions
	if country == "Argentina" and name == "Buenos Aires":
		admin1 = "-"
	if country == "Venezuela" and name == "Caracas":
		admin1 = "Caracas"
	if country == "Mexico" and name == 'Ciudad de M\xe9xico'.decode('unicode-escape'):
		admin1 = 'Ciudad de M\xe9xico'.decode('unicode-escape')


	altTokens = altNames.split(' ')
	alt = ['', '', '']
	for i in range(0, 3):
		if len(altTokens) > i + 1:
			alt[i] = altTokens[i]

	# target output format: id, name, alt1, alt2,  type, pop, lat, long, country, admin1, admin2
	fout.write('\t'.join((uniqueID, name, alt[0], alt[1], geoType, population, latitude, longitude, country, admin1, admin2)))
	fout.write('\n')

	if name not in name2id:
		name2id[name] = set()
	name2id[name].add(uniqueID)

	id2pop[uniqueID] = float(population)

	num += 1

print num

fout.close()


fout = open('refersToLower-full.txt', 'w')

for name in name2id:
	total = 0
	for place in name2id[name]:
		total += id2pop[place]
	for place in name2id[name]:
		if total == 0:
			total = 1

		norm_name = unicodedata.normalize('NFKD', name).encode('ascii','ignore').lower()
		fout.write("%s\t%s\t%f\n" % (norm_name, place, id2pop[place] / total))

fout.close()



