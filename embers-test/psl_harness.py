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

log = logs.getLogger('psl_harness')

"""
psl_harness.py

Arguments (required):
--sub		the feed to subscribe to
--pub		the feed to publish to
--model		the PSL model to run
--project	where the Eclipse/Maven project for the model is

Arguments (optional):
--ssh_key		the private key to use to tunnel to EMBERS
--tunnel		the host to tunnel to
--msg_folder	where to write feed/queue messages to when running PSL
--result_folder	where PSL will write the results for a message

psl_harness.py will:
- Continuously read from a queue
- Write the message it receives to a folder (titled by the embersId)
- Call PSL (not implemented) with the argument of the message file
- Wait for PSL to complete
- Read the result file (titled by the embersId) from the result folder
- Publish that result to a queue

"""
def main():
	# Initialize arguments
	argparser = args.get_parser()
	argparser.add_argument('--msg_folder', help='Where to write messages to')
	argparser.add_argument('--result_folder', help='Where PSL results will reside')
	argparser.add_argument('--project', help='Where the PSL model is located', required=True)
	argparser.add_argument('--model', help='The PSL model to run (ex. edu.umd.cs.linqs.embers.RSSLocationPredictor)', required=True)
	arg = argparser.parse_args()
	
	# The output folder for PSL messages and results.
	message_folder = "./messages/"
	results_folder = "./results/"
	if arg.msg_folder:
		message_folder = arg.msg_folder
	if arg.result_folder:
		results_folder = arg.result_folder
	
	# Initialize log
	logs.init(arg)
	global log
	
	# Generate a classpath for the PSL program
	project_folder = arg.project
	os.chdir(project_folder)
	subprocess.call(['mvn', 'compile'])
	subprocess.call(['mvn', 'dependency:build-classpath', '-Dmdep.outputFile=classpath.out'])
	
	# Read in classpath to string
	classpath = open(os.path.join(project_folder, 'classpath.out'), 'r').readline()
	classpath += ":" + os.path.join(project_folder, 'target/classes/')
	log.info("Generated classpath for project %s. Classpath: %s", arg.model, classpath)
	
	# Initialize the queue with arguments and connect to the specified feed
	log.info("Opening and connecting to queue %s", arg.sub)
	queue.init(arg)
	reader = queue.open(arg.sub, 'sub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	
	# Initialize the writer to publish to a queue
	log.info("Publishing to queue %s", arg.pub)
	writer = queue.open(arg.pub, 'pub', ssh_key=arg.ssh_key, ssh_conn=arg.tunnel)
	
	# Now launch PSL
	for feedmsg in reader:
		# Clean the message to fix irregularities
		feedmsg = message.clean(json.loads(feedmsg))
		
		# Write out message to messages folder
		message_file = codecs.open(os.path.join(message_folder, feedmsg['embersId']), encoding='utf-8', mode='w')
		message_file.write(json.dumps(feedmsg))
		message_file.close()
		
		# Launch PSL, telling it the name of the file to analyze
		log.info("Launching PSL instance for message. [%s]", feedmsg['embersId'])
		message_file = os.path.abspath(os.path.join(message_folder, feedmsg['embersId']))
		subprocess.call(["java", "-Dfile.encoding=UTF-8", "-classpath", classpath, arg.model, message_file])
		
		# Now publish the result
		results_file = codecs.open(os.path.join(results_folder, feedmsg['embersId']), encoding='utf-8', mode='r')
		result = json.load(results_file)
		result = message.add_embers_ids(result)
		log.info("Publishing result message. [new id: %s]", result['embersId'])
		writer.write(json.dumps(result))

if __name__ == '__main__':
	main()

