The following command will start a test instance of PSL harness, 

python psl_harness.py --sub tcp://127.0.0.1:1234 --pub tcp://127.0.0.1:4321 --msg_folder ../psl-rss/messages/ --result_folder ../psl-rss/results/ --model edu.umd.cs.linqs.embers.RSSLocationPredictor --project ../psl-rss/ --keep_files

This runs the RSSLocationPredictor PSL program, watches for EMBERS RSS messages on the local port 1234, and publishes a PSL-enriched feed to port 4321

To run through the simulated feed of the GSR-labeled rss articles, use the following command:

python test_publisher.py --pub tcp://127.0.0.1:1234 --json_file ../psl-rss/aux_data/januaryGSRGeoCode.json 

From psl-rss/, run 

java -cp ./target/classes:`cat classpath.out` edu.umd.cs.linqs.embers.ResultsEvaluator

to evaluate geolocation accuracy against the GSR.

On production runs of psl_harness.py, omit --keep_files to save disk space. 
