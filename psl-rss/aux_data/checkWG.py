#!/usr/bin/python

import codecs

fullCities = set()
fullStates = set()
fullCountries = set()

laCities = set()
laStates = set()
laCountries = set()

minPop = 100000000
for line in codecs.open('wg-full-parsed.txt', 'r', encoding = 'utf-8'):
	tokens = line.strip('\n').split('\t')
	fullCities.add(tokens[1])
	fullStates.add(tokens[9])
	fullCountries.add(tokens[8])
	pop = int(tokens[5])
	if pop < minPop:
		minPop = pop


for line in codecs.open('wg-partial.orig.txt', 'r', encoding = 'utf-8'):
	tokens = line.strip('\n').split('\t')
	laCities.add(tokens[1])
	laStates.add(tokens[9])
	laCountries.add(tokens[8])

print "Min pop %d" % minPop


missingCities = [o for o in laCities if o not in fullCities]

print "Cities %s" % missingCities


missingStates = [o for o in laStates if o not in fullStates]

print "States missing %d, la %d, full %d, %s" % (len(missingStates), len(laStates), len(fullStates), missingStates)

missingCountries = [o for o in laCountries if o not in fullCountries]

print "Countries %s" % missingCountries


laPlaces = set()
fullPlaces = set()

for line in codecs.open('refersToLower-full.txt'):
	tokens = line.strip('\n').split('\t')
	fullPlaces.add(tokens[0])
	
for line in codecs.open('refersToLower.txt'):
	tokens = line.strip('\n').split('\t')
	laPlaces.add(tokens[0])

missingPlaces = [o for o in laPlaces if o not in fullPlaces]

print "Places missing %d, la %d, full %d, %s" % (len(missingPlaces), len(laPlaces), len(fullPlaces), missingPlaces)


