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

class TwitterAnalyzer:
	def __init__(self, outdir, keywords):
		self.tweetContent = codecs.open(os.path.join(outdir, 'tweetContent.txt'), encoding='utf-8', mode='w')
		self.tweetUser = codecs.open(os.path.join(outdir, 'tweetUser.txt'), encoding='utf-8', mode='w')
		self.negativeSentiment = codecs.open(os.path.join(outdir, 'negativeSentiment.txt'), encoding='utf-8', mode='w')
		self.positiveSentiment = codecs.open(os.path.join(outdir, 'positiveSentiment.txt'), encoding='utf-8', mode='w')
		self.containsHashtag = codecs.open(os.path.join(outdir, 'containsHashtag.txt'), encoding='utf-8', mode='w')
		self.tweetPlace = codecs.open(os.path.join(outdir, 'tweetPlace.txt'), encoding='utf-8', mode='w')
		self.tweetGeocode = codecs.open(os.path.join(outdir, 'tweetGeocode.txt'), encoding='utf-8', mode='w')
		self.userLocation = codecs.open(os.path.join(outdir, 'userLocation.txt'), encoding='utf-8', mode='w')
		self.mentions = codecs.open(os.path.join(outdir, 'mentions.txt'), encoding='utf-8', mode='w')
		self.retweet = codecs.open(os.path.join(outdir, 'retweet.txt'), encoding='utf-8', mode='w')
		self.containsKeyword = codecs.open(os.path.join(outdir, 'containsKeyword.txt'), encoding='utf-8', mode='w')
		self.followers = codecs.open(os.path.join(outdir, 'followers.txt'), encoding='utf-8', mode='w')
		self.username = codecs.open(os.path.join(outdir, 'usernames.txt'), encoding='utf-8', mode='w')
		self.keywords = keywords
	
	def process_file(self, filename):
		print "Processing: " + filename
		inputfile = codecs.open(filename, encoding='utf-8')
		if filename.endswith('.txt'):
			self.__process_twitter(inputfile)
		elif filename.endswith('.dir'):
			self.__process_followers(inputfile)
		else:
			print "...not a .txt or .dir, skipping."
		
	def close(self):
		# Close files
		self.tweetContent.close()
		self.tweetUser.close()
		self.negativeSentiment.close()
		self.positiveSentiment.close()
		self.containsHashtag.close()
		self.tweetPlace.close()
		self.tweetGeocode.close()
		self.userLocation.close()
		self.mentions.close()
		self.retweet.close()
		self.containsKeyword.close()
	
	def __process_twitter(self, inputfile):
		seenTweets = set()
		seenUsers = set()
		
		# RegEx for detecting hashtags
		hashtagPattern = re.compile('[#]+[A-Za-z0-9-_]+')
		
		for line in inputfile:
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
			self.tweetContent.write(tweetID + '\t' + content + '\n')
			
			# Write out tweetID, user
			user = str(tweet['interaction']['author']['id'])
			self.tweetUser.write(tweetID + '\t' + user + '\n')
			
			# Write out the userID, username (could be more than one mapping)
			self.username.write(user + '\t' + tweet['interaction']['author']['username'] + '\n')
			
			# Write out the sentiment
			if tweet.has_key('salience'):
				sentiment = float(tweet['salience']['content']['sentiment'])
				normalized = sentiment / 20
				if (normalized < 0):
					self.negativeSentiment.write(tweetID + '\t' + str(abs(normalized)) + '\n')
				elif (normalized > 0):
					self.positiveSentiment.write(tweetID + '\t' + str(normalized) + '\n')
				# Do not write out zero-valued sentiments
			
			# Write out hashtag information
			foundTags = dict()
			for match in hashtagPattern.findall(content):
				if not foundTags.has_key(match):
					self.containsHashtag.write(tweetID + '\t' + match + '\n')
					foundTags[match] = True
			
			# Write out tweet place information
			if tweet['twitter'].has_key('place'):
				self.tweetPlace.write(tweetID + '\t' + tweet['twitter']['place']['full_name'] + '\n')
			
			# Write out tweet geolocation information
			if tweet['twitter'].has_key('geo'):
				latitude = tweet['twitter']['geo']['latitude']
				longitude = tweet['twitter']['geo']['longitude']
				self.tweetGeocode.write(tweetID + '\t' + str(latitude) + ',' + str(longitude) + '\n')
			
			# Write out users' location information
			if user not in seenUsers:
				seenUsers.add(user)
				if tweet['twitter'].has_key('user'):
					if (tweet['twitter']['user'].has_key('location')):
						location = re.sub('[\n\t]', ' ', tweet['twitter']['user']['location'])
						if len(location.strip()) > 0:
							self.userLocation.write(user + '\t' + location + '\n')
						else:
							print "Found user with all whitespace location"
			
			# Write out mentions information
			if tweet['twitter'].has_key('mentions'):
				for otherUser in tweet['twitter']['mentions']:
					self.mentions.write(tweetID + '\t' + otherUser + '\n')
			
			# Write out retweet information
			if tweet['twitter'].has_key('retweeted'):
				originalAuthor = str(tweet['twitter']['retweeted']['user']['id'])
				self.retweet.write(tweetID + '\t' + originalAuthor + '\n')
			
			# Write out keywords
			cleanedContent = re.sub("[^_a-zA-Z0-9\s]", "", content.strip().lower().encode('ascii', 'ignore'))
			tokens = cleanedContent.split()
			seenWords = set()
			for word in tokens:
				if word in self.keywords and word not in seenWords:
					self.containsKeyword.write(tweetID + '\t' + word + '\n')
					seenWords.add(word)
			
			# Finished
	
	def __process_followers(self, inputfile):
		userPattern = re.compile('(\w+) (\d+)')
		user = None
		for line in inputfile:
			if user is None:
				match = userPattern.match(line)
				user = match.group(1)
				count = int(match.group(2))
			else:
				count -= 1
				self.followers.write(user + '\t' + line.strip() + '\n')
			
			if count == 0:
				user = None

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("data_directory", help="the data directory of the EMBERS OSI twitter data")
	parser.add_argument("output_directory", help="the directory to output predicates")
	parser.add_argument("keyword_file", help="file containing keywords to check for")
	
	args = parser.parse_args()
	
	directory = args.data_directory
	output = args.output_directory
	keywordfile = args.keyword_file
	print "Data directory: " + directory
	print "Output directory: " + output
	
	# load keywords
	keywords = set([word.strip() for word in open(keywordfile)])
	
	analyzer = TwitterAnalyzer(output, keywords)
	
	for inputfile in os.listdir(directory):
		analyzer.process_file(os.path.join(directory, inputfile))
	
	analyzer.close()
	
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
