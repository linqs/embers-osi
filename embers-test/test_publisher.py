#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  test_publisher.py
#  
#  Copyright 2013 Eric Tyler Norris <enorris@pebble>
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

from etool import args, logs, queue
import os
import codecs
import time

log = logs.getLogger('test-publisher')

"""
test_publisher.py

Arguments (required):
--pub		the feed to publish to
--json_file	the JSON file to publish messages to

Arguments (optional):
--ssh_key	the private key to use to tunnel to EMBERS
--tunnel	the host to tunnel to

test_publisher.py will:
- Continuously read from a file
- Publish each JSON message to the specified queue
- once it reaches EOF, start again
"""
def main():
	# Initialize arguments
	argparser = args.get_parser()
	argparser.add_argument('--json_file', help='JSON file to publish', required=True)
	arg = argparser.parse_args()
	
	queue.init(arg)
	writer = queue.open(arg.pub, 'pub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	#reader = queue.open(arg.sub, 'sub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	
	try:
		while True:
			msg_reader = codecs.open(arg.json_file, encoding='utf-8', mode='r')
			message = msg_reader.readline()
			while message:
				writer.write(message)
				message = msg_reader.readline()
			
			msg_reader.close()
			time.sleep(3)
	except KeyboardInterrupt:
		pass
	
	return 0

if __name__ == '__main__':
	main()

