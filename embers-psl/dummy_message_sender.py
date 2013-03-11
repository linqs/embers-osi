#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  psl_harness.py
#  
#  Copyright 2013 Bert Huang
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

import os
import codecs
import json
import subprocess
import socket
from time import sleep
import linecache

"""
psl_harness.py

Arguments (required):
--sub			the feed to subscribe to
--pub			the feed to publish to
--local_port	local port to forward and receive messages 

Arguments (optional):
--ssh_key		the private key to use to tunnel to EMBERS
--tunnel		the host to tunnel to

psl_harness.py will:
- Continuously read from a queue
- Forward any messages from read queue to a socket on local_port
- Read response from local_port
- Publish that result to a queue

"""
def main():
	# Initialize arguments

	localPort = 9999	
	count = 0
	# Connect to Java server
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	message = linecache.getline('../psl-rss/aux_data/januaryGSRGeoCode.json', 1).rstrip()
	# message = '{"BasisEnrichment":{"entities":[{"neType":"LOCATION","expr":"Caracas","offset":"0:1"}],"language":"English"}, "embersId":"12345"}'
	while True:
		while True:
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect(("localhost", localPort))
				break
			except:
				print "unable to connect. Retrying in 3 seconds"
				sleep(3)

		try:
			socketLines = sock.makefile()
			# Write message to socket stream
			sock.sendall(message)
			sock.sendall('\n')

			# Receive result from socket stream
			result = socketLines.readline()
		except KeyboardInterrupt:
			sys.exit(1)

	sock.close()



if __name__ == '__main__':
	main()
