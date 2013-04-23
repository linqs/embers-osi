#/usr/bin/python

import re
import sys

lowerWords = set()
for line in open('wg-partial.orig.txt', 'r'):
	tokens = line.strip().split('\t')
	
	for token in tokens:
		words = token.decode('utf-8').split(' ')
		for word in words:	
			if not re.findall("[0-9]", word):
				if word == word.lower():
					lowerWords.add(word)


print lowerWords