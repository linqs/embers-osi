PSL Harness and PSL Geocoding Model
=====================

Requirements
--------------

Running the PSL harness requires python, java and maven 3. 


Installation
-------------
Currently, the installation process is:

1. in ``psl/`` run, ``install_psl.sh``: this will use maven to install the PSL binaries in the maven repository (default ``~/.m2``)

2. Set up config files:
	
  * ``psl/psl-rss/src/main/resources/log4j.properties`` sets up PSL's logging.

  * ``psl/psl-rss/src/main/resources/psl.properties`` contains various settings for PSL. Most important are:

    * ``rss.dbpath`` file system path where H2 database file will be stored

    * ``rss.auxdatapath`` file system path where gazatteer, and precomputed predicates are stored (currently psl/psl-rss/aux_data)

    * ``rss.jsonserver.port`` local port to send messages from python to java

    * ``rss.jsonserver.processor`` name of PSL program to run on messages (currently RSSLocationProcessor)

    * ``rss.rsslocationprocessor.outputdir`` directory to dump json text files of PSL-predicted locations. Useful for offline evaluation, but it's best to comment this out for production so no files will be output.


Instructions for PSL Geocoding
------------------------------

1. In a separate shell, start the java server by running the script ``psl/psl-rss/start_PSL_geocoding.sh``. This will compile the java and groovy code, do some initial preprocessesing loading the gazatteer and auxiliary data into the database and then start the server. 

2. Start the python harness 

	python embers-psl/psl_harness.py --sub tcp://<hostname>:<incoming port> --pub tcp://<hostname>:<outgoing port> --local port <port to send messages to java>

3. Wait until something goes wrong

4. Email bert@cs.umd.edu for help with what went wrong

Instructions for offline evaluation
-----------------------------------

0. To evaluate this system, we can run on the January GSR news articles. Do this by first uncommenting the option ``rss.rsslocationprocessor.outputdir`` and set it to ``./results/``. 

1. Start the psl harness with these options to listen to local ports:

	python psl_harness.py --pub <anything> --sub tcp://127.0.0.1:1234 --local_port 9999

2. And then in a separate shell, start the test feed script

	python test_publisher.py --pub tcp://127.0.0.1:1234 --json_file ../psl-rss/aux_data/januaryGSRGeocode.json

This should process all the January planned protest events.

3. Evaluate against the old system with 

	java -cp ./target/classes:`cat classpath.out` edu.umd.cs.linqs.embers.ResultsEvaluator

	