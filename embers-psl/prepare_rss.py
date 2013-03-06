#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  prepare_rss.py
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

from etool import args
import os
import subprocess

def main():
	argparser = args.get_parser()
	argparser.add_argument('--project', help='Where the PSL model is located', required=True)
	argparser.add_argument('--script', help='The preparation file to run', required=True)

	# Generate a classpath for the PSL program
	project_folder = arg.project
	os.chdir(project_folder)
	subprocess.call(['mvn', 'compile'])
	subprocess.call(['mvn', 'dependency:build-classpath', '-Dmdep.outputFile=classpath.out'])
	
	# Read in classpath to string
	classpath = open(os.path.join(project_folder, 'classpath.out'), 'r').readline()
	classpath += ":" + os.path.join(project_folder, 'target/classes/')
	log.info("Generated classpath for project %s. Classpath: %s", arg.model, classpath)
	
	subprocess.call(["java", "-Dfile.encoding=UTF-8", "-classpath", classpath, arg.script])
	
	return 0

if __name__ == '__main__':
	main()

