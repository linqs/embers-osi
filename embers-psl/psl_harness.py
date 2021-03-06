#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  psl_harness.py
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

from etool import args, logs, queue, message
import os
import codecs
import json
import subprocess
import socket
from time import sleep

log = logs.getLogger('psl_harness')

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
	argparser = args.get_parser()
	argparser.add_argument('--local_port', help='Local port to connect to java server', required=True)
	arg = argparser.parse_args()
		
	localPort = int(arg.local_port)

	# Initialize log
	logs.init(arg)
	global log
	
	# Initialize the queue with arguments and connect to the specified feed
	log.info("Opening and connecting to queue %s", arg.sub)
	queue.init(arg)
	reader = queue.open(arg.sub, 'sub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	
	# Initialize the writer to publish to a queue
	log.info("Publishing to queue %s", arg.pub)
	writer = queue.open(arg.pub, 'pub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	

	count = 0
	# Connect to Java server
	while True:
		for feedmsg in reader:
			try:
				while True:
					try:
						sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sock.connect(("localhost", localPort))
						break
					except:
						log.info("Unable to connect to local server")

				log.debug("Connected to java server on port %d" % localPort)

				socketLines = sock.makefile()

				# Clean the message to fix irregularities
				feedmsg = message.clean(feedmsg)

				log.debug("Read message %d. Sending to java" % count)
				# Write message to socket stream
				sock.sendall(json.dumps(feedmsg))
				sock.sendall('\n')

				# Receive result from socket stream
				result = socketLines.readline()
				writer.write(json.dumps(result))
				count += 1

				sock.close()
			except KeyboardInterrupt:
				sys.exit(1)
			else:
				log.info("Server was disconnected.")




if __name__ == '__main__':
	main()
