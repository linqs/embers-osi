#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  geocode_normalizer.py
#  
#  Copyright 2013 Eric Norris <enorris@cs.umd.edu>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
#  

import argparse
import operator
import re
from collections import defaultdict

# from http://code.activestate.com/recipes/278258-list-tools/
def normListSumTo(L, sumTo=1):
    '''normalize values of a list to make it sum = sumTo'''
    sum = reduce(lambda x,y:x+y, L)
    return [ x/(sum*1.0)*sumTo for x in L]

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("geolocation_prediction", help="the file containing the predictions of geolocations")
	parser.add_argument("output_file", help="the destination for the averaged geolocations")
	args = parser.parse_args()
	
	geofile = args.geolocation_prediction
	predictions = defaultdict(list)
	results = dict()
	
	for line in open(geofile):
		tokens = re.split('\t', line)
		# Get EmbersID for tweet
		embersID = tokens[0]
		
		# Parse geolocation
		coordinates = re.split(',', tokens[1][1:-1])
		coordinates = tuple(map(float, coordinates))
		
		# Parse truth value
		weight = float(tokens[2])
		
		# Store non-zero weights in dictionary
		if (weight != 0.0):
			predictions[embersID].append((coordinates, weight))
	
	for embersID in predictions.iterkeys():
		predictionList = predictions[embersID]
		
		# Normalize the weights
		normalized = normListSumTo([x[1] for x in predictionList])
		
		# Overwrite the non-normal weights
		predictionList = zip([x[0] for x in predictionList], normalized)
		
		# Now calculate weighted average
		result = (0.0, 0.0)
		for coordinate, weight in predictionList:
			weightedCoordinate = tuple([weight * x for x in coordinate])
			result = tuple(map(operator.add, result, weightedCoordinate))
		
		# Store the result
		results[embersID] = result
	
	# Write out result to file
	output = args.output_file
	outputFile = open(output, 'w')
	
	for embersID in results.iterkeys():
		outputFile.write('{}\t{}\n'.format(embersID, results[embersID]))
	
	return 0

if __name__ == '__main__':
	main()

