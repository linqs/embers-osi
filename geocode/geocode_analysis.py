#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  geocode_analysis.py
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
import math
import os
import sys
import inspect
import re
import ast

# Import geopy library from subfolder
# http://stackoverflow.com/questions/279237/python-import-a-module-from-a-folder
cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile(inspect.currentframe()))[0], "geopy-library")))
if cmd_subfolder not in sys.path:
	sys.path.insert(0, cmd_subfolder)
	
from geopy import distance

def distance_km(pointA, pointB):
	return distance.distance(pointA, pointB).km

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("ground_truth", help="the file containing the ACTUAL geolocations")
	parser.add_argument("normalized_prediction", help="the file containing the normalized (averaged) predictions of geolocations")
	parser.add_argument("output_file", help="the destination for the difference between the input files")
	args = parser.parse_args()
	
	truthfile = args.ground_truth
	predictionfile = args.normalized_prediction
	truth = dict()
	predictions = dict()
	
	# Parse predicted locations
	for line in open(truthfile):
		tokens = re.split('\t', line)
		# Get EmbersID for tweet
		embersID = tokens[0]
		
		# Parse geolocation
		#coordinates = re.split(',', tokens[1][1:-1])
		#coordinates = tuple(map(float, coordinates))
		coordinates = ast.literal_eval(tokens[1])
		
		# Store
		truth[embersID] = coordinates
	
	# Parse predicted locations
	for line in open(predictionfile):
		tokens = re.split('\t', line)
		# Get EmbersID for tweet
		embersID = tokens[0]
		
		# Parse geolocation
		#coordinates = re.split(',', tokens[1][1:-1])
		#coordinates = tuple(map(float, coordinates))
		coordinates = ast.literal_eval(tokens[1])
		
		# Store
		predictions[embersID] = coordinates
	
	results = dict([(id, distance_km(truth[id], predictions[id])) for id in truth.iterkeys() if predictions.has_key(id)])
	
	# Calculate average + std. dev
	avg = sum(results.itervalues()) / len(results)
	std = sum([(x - avg) ** 2 for x in results.itervalues()])
	std = math.sqrt(std / float(len(results) - 1))
	
	# Calculate true positives, false postives, and false negatives
	predictionSet = set(predictions.viewkeys())
	truthSet = set(truth.viewkeys())
	tp = float(len(predictionSet.intersection(truthSet)))
	fp = float(len(predictionSet.difference(truthSet)))
	fn = float(len(truthSet.difference(predictionSet)))
	
	# Calculate precision and recall
	precision = tp / (tp + fp)
	recall = tp / (tp + fn)
	
	# Print result calculations
	print "Average: \t%f" % avg
	print "Std. Dev: \t%f" % std
	print "Precidion: \t%f" % precision
	print "Recall: \t%f" % recall
	
	# Write out difference to file
	output = args.output_file
	outputFile = open(output, 'w')
	
	for embersID in results.iterkeys():
		outputFile.write('{}\t{}\n'.format(embersID, results[embersID]))
	
	return 0

if __name__ == '__main__':
	main()

