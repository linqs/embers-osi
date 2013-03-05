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
--sub	the feed to subscribe to
--pub	the feed to publish to

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
		os.chdir("/linqshomes/enorris/workspace/embers-osi/psl-rss/")
		subprocess.call(["/usr/lib/jvm/java-6-openjdk-amd64/bin/java", "-Dfile.encoding=UTF-8", "-classpath", "/linqshomes/enorris/workspace/embers-osi/psl-rss/target/classes:/linqshomes/enorris/.m2/repository/org/json/json/20090211/json-20090211.jar:/linqshomes/enorris/workspace/psl/psl-groovy/target/test-classes:/linqshomes/enorris/workspace/psl/psl-groovy/target/classes:/linqshomes/enorris/.m2/repository/org/codehaus/groovy/groovy-eclipse-batch/1.8.0-03/groovy-eclipse-batch-1.8.0-03.jar:/linqshomes/enorris/workspace/psl/psl-core/target/test-classes:/linqshomes/enorris/workspace/psl/psl-core/target/classes:/linqshomes/enorris/.m2/repository/net/sourceforge/collections/collections-generic/4.01/collections-generic-4.01.jar:/linqshomes/enorris/.m2/repository/com/healthmarketscience/common/common-util/1.0.2/common-util-1.0.2.jar:/linqshomes/enorris/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar:/linqshomes/enorris/.m2/repository/commons-beanutils/commons-beanutils-core/1.8.0/commons-beanutils-core-1.8.0.jar:/linqshomes/enorris/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar:/linqshomes/enorris/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar:/linqshomes/enorris/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar:/linqshomes/enorris/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar:/linqshomes/enorris/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:/linqshomes/enorris/.m2/repository/edu/emory/mathcs/csparsej/1.0/csparsej-1.0.jar:/linqshomes/enorris/.m2/repository/com/google/guava/guava/r09/guava-r09.jar:/linqshomes/enorris/.m2/repository/com/h2database/h2/1.2.126/h2-1.2.126.jar:/linqshomes/enorris/.m2/repository/edu/emory/mathcs/jplasma/1.2/jplasma-1.2.jar:/linqshomes/enorris/.m2/repository/junit/junit/4.5/junit-4.5.jar:/linqshomes/enorris/.m2/repository/log4j/log4j/1.2.16/log4j-1.2.16.jar:/linqshomes/enorris/.m2/repository/de/mathnbits/mathnbitsSTL/1.0/mathnbitsSTL-1.0.jar:/linqshomes/enorris/.m2/repository/mysql/mysql-connector-java/5.0.5/mysql-connector-java-5.0.5.jar:/linqshomes/enorris/.m2/repository/edu/emory/mathcs/parallelcolt/0.9.4/parallelcolt-0.9.4.jar:/linqshomes/enorris/.m2/repository/org/slf4j/slf4j-api/1.5.8/slf4j-api-1.5.8.jar:/linqshomes/enorris/.m2/repository/org/slf4j/slf4j-log4j12/1.5.8/slf4j-log4j12-1.5.8.jar:/linqshomes/enorris/.m2/repository/com/healthmarketscience/sqlbuilder/sqlbuilder/2.0.6/sqlbuilder-2.0.6.jar:/linqshomes/enorris/.m2/repository/org/ujmp/ujmp-complete/0.2.4/ujmp-complete-0.2.4.jar:/linqshomes/enorris/Downloads/jsr166.jar:/linqshomes/enorris/.groovy/greclipse/global_dsld_support:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/plugin_dsld_support/:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/groovy-all-2.0.4.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/jline-1.0.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/servlet-api-2.4.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/asm-4.0.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/asm-tree-4.0.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/antlr-2.7.7.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/commons-cli-1.2.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/bsf-2.4.0.jar:/linqshomes/enorris/Downloads/eclipse/plugins/org.codehaus.groovy_2.0.4.xx-20120921-2000-e42RELEASE/lib/ivy-2.2.0.jar", "edu.umd.cs.linqs.embers.RSSLocationPredictor", message_file])
		
		# Now publish the result
		results_file = codecs.open(os.path.join(results_folder, feedmsg['embersId']), encoding='utf-8', mode='r')
		result = json.load(results_file)
		result = message.add_embers_ids(result)
		log.info("Publishing result message. [new id: %s]", result['embersId'])
		#writer.write(json.dump(result))

if __name__ == '__main__':
	main()

