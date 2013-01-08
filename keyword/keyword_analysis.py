#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  untitled.py
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
import re

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("keyword_predictions", help="file containing predicted keywords")
	parser.add_argument("keyword_file", help="file containing actual keywords")
	args = parser.parse_args()
	
	prediction_file = args.keyword_predictions
	keyword_file = args.keyword_file
	
	# Read in predictions
	predictions = [tuple(re.split('\t', line)) for line in open(prediction_file)]
	
	# Read in actual keywords
	keywords = set([word.strip() for word in open(keyword_file)])
	
	results = dict()
	
	print 'Threshold:\tPrecision:\tRecall'
	# Iterate over range of thresholds (from 0.1 to 1.0)
	for threshold in [x * 0.1 for x in range(1, 11)]:
		# Look at only keywords that fit the threshold
		predictionSet = set([keyword for keyword, truth in predictions if float(truth) > threshold])

		if len(predictionSet) == 0:
			print '%.1f\t\t%.5f\t\t%.5f' % (threshold,0.0,0.0)
			continue

		# Calculate true positives, false postives, etc
		tp = float(len(predictionSet.intersection(keywords)))
		fp = float(len(predictionSet.difference(keywords)))
		fn = float(len(keywords.difference(predictionSet)))
		
		# Calculate precision and recall
		precision = tp / (tp + fp)
		recall = tp / (tp + fn)
		
		# Pretty print result
		print '%.1f\t\t%.5f\t\t%.5f' % (threshold,precision,recall)
	return 0

if __name__ == '__main__':
	main()

