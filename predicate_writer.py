#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  predicate_writer.py
#  
#  Copyright 2012 Eric Norris <enorris@umd.cs.edu>
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

import sys
import string
import getopt
import argparse
import os
import json
import codecs
import re

def process_file(filename, outdir, keywords):
	print "Processing: " + filename
	file = codecs.open(filename, encoding = 'utf-8')
	if filename.endswith('.txt'):
		process_twitter(file, outdir, keywords)
	elif filename.endswith('.dir'):
		process_followers(file, outdir)
	else:
		print "...not a .txt or .dir, skipping."
		

def process_twitter(file, outdir, keywords):
	tweetContent = codecs.open(os.path.join(outdir, 'tweetContent.txt'),
		encoding = 'utf-8', mode = 'w')
	tweetUser = codecs.open(os.path.join(outdir, 'tweetUser.txt'),
		encoding = 'utf-8', mode = 'w')
	positiveSentiment = codecs.open(os.path.join(outdir, 'positiveSentiment.txt'),
		encoding = 'utf-8', mode = 'w')
	negativeSentiment = codecs.open(os.path.join(outdir, 'negativeSentiment.txt'),
		encoding = 'utf-8', mode = 'w')
	containsHashtag = codecs.open(os.path.join(outdir, 'containsHashtag.txt'),
		encoding = 'utf-8', mode = 'w')
	tweetPlace = codecs.open(os.path.join(outdir, 'tweetPlace.txt'),
		encoding = 'utf-8', mode = 'w')
	tweetGeocode = codecs.open(os.path.join(outdir, 'tweetGeocode.txt'),
		encoding = 'utf-8', mode = 'w')
	userLocation = codecs.open(os.path.join(outdir, 'userLocation.txt'),
		encoding = 'utf-8', mode = 'w')
	mentions = codecs.open(os.path.join(outdir, 'mentions.txt'),
		encoding = 'utf-8', mode = 'w')
	retweet = codecs.open(os.path.join(outdir, 'retweet.txt'),
		encoding = 'utf-8', mode = 'w')
	containsKeyword = codecs.open(os.path.join(outdir, 'containsKeyword.txt'),
		encoding = 'utf-8', mode = 'w')

	seenTweets = set()

	seenUsers = set()

	for line in file:
		# Read in line and parse as JSON
		try:
			tweet = json.loads(line)
		except ValueError:
			print "Could not read entry"
			print line
			continue
		
		# Pull out ID for tweet
		if tweet.has_key('embers_id'):
			tweetID = tweet['embers_id']
		elif tweet.has_key('embersId'):
			tweetID = tweet['embersId']
		else:
			tweetID = tweet['embersID']

		if tweetID in seenTweets:
			print "Already saw tweet %s" % tweetID
			continue
		else:
			seenTweets.add(tweetID)
		
		# Write out tweetID, tweet
		content = re.sub('[\n\t]', ' ', tweet['interaction']['content'])
		tweetContent.write(tweetID + '\t' + content + '\n')
		
		# Write out tweetID, user
		user = tweet['interaction']['author']['username']
		tweetUser.write(tweetID + '\t' + user + '\n')
		
		# Write out the sentiment
		if tweet.has_key('salience'):
			sentiment = float(tweet['salience']['content']['sentiment'])
			normalized = sentiment / 20
			if (normalized < 0):
				negativeSentiment.write(tweetID + '\t' + str(abs(normalized)) + '\n')
			elif (normalized > 0):
				positiveSentiment.write(tweetID + '\t' + str(normalized) + '\n')
			# Do not write out zero-valued sentiments
		
		# Write out hashtag information
		hashtagPattern = re.compile('[#]+[A-Za-z0-9-_]+')
		foundTags = dict()
		for match in hashtagPattern.findall(content):
			if not foundTags.has_key(match):
				containsHashtag.write(tweetID + '\t' + match + '\n')
				foundTags[match] = True
		
		# Write out tweet place information
		if tweet['twitter'].has_key('place'):
			 tweetPlace.write(tweetID + '\t' + tweet['twitter']['place']['full_name'] + '\n')
		
		# Write out tweet geolocation information
		if tweet['twitter'].has_key('geo'):
			latitude = tweet['twitter']['geo']['latitude']
			longitude = tweet['twitter']['geo']['longitude']
			tweetGeocode.write(tweetID + '\t' + str(latitude) + ',' + str(longitude) + '\n')
		
		# Write out users' location information
		if user not in seenUsers:
			seenUsers.add(user)
			if tweet['twitter'].has_key('user'):
				if (tweet['twitter']['user'].has_key('location')):
					location = re.sub('[\n\t]', ' ', tweet['twitter']['user']['location'])
					if len(location.strip()) > 0:
						userLocation.write(user + '\t' + location + '\n')
					else:
						print "Found user with all whitespace location"
		
		# Write out mentions information
		if tweet['twitter'].has_key('mentions'):
			for otherUser in tweet['twitter']['mentions']:
				mentions.write(tweetID + '\t' + otherUser + '\n')
		
		# Write out retweet information
		if tweet['twitter'].has_key('retweeted'):
			originalAuthor = tweet['twitter']['retweeted']['user']['screen_name']
			retweet.write(tweetID + '\t' + originalAuthor + '\n')
		
		# Write out keywords
		cleanedContent = re.sub("[^_a-zA-Z0-9\s]", "", content.strip().lower().encode('ascii', 'ignore'))
		tokens = cleanedContent.split()
		seenWords = set()
 		for word in tokens:
			if word in keywords and word not in seenWords:
				containsKeyword.write(tweetID + '\t' + word + '\n')
				seenWords.add(word)

	
def process_followers(file, outdir):
	# TODO: Implement follower graph parsing.
	print "TODO: Implement follower graph parsing."
	return 1;

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("data_directory", help="the data directory of the EMBERS OSI twitter data")
	parser.add_argument("output_directory", help="the directory to output predicates")
	parser.add_argument("keyword_file", help="file containing keywords to check for")
	
	args = parser.parse_args();
	
	directory = args.data_directory
	output = args.output_directory
	keywordFile = args.keyword_file
	print "Data directory: " + directory
	print "Output directory: " + output
	
	# load keywords
	keywords = set()
	for word in open(keywordFile, 'r'):
		keywords.add(word.strip())
	
	for file in os.listdir(directory):
		process_file(os.path.join(directory, file), output, keywords)

	return 0;

if __name__ == '__main__':
	main()


# 1) iterate through each file in the folder
# 2) if it ends in .txt:
#		- parse it as JSON
#		- desired data:
#			- tweetID, tweet
#			- tweetID, user (tweeted)
#			- tweetID, normalized POSITIVE sentiment
#			- tweetID, normalized NEGATIVE sentiment
#			- tweetID, hashtag (containsHashtag)
#			- tweetID, place
#			- tweetID, geolocation
#			- userID, location
#			- tweetID, username (mentions)
#			- tweetID, username (retweeted)
# 3) if it ends in .dir:
#		- parse it as follower graph
#		- userID, follower
