
echo copying from $1

cp $1/psl/README.md README.md 
cp $1/psl/install_psl.sh install_psl.sh 
cp -r $1/psl/psl-1.1-SNAPSHOT ./
cp $1/psl/embers-psl/psl_harness.py embers-psl/psl_harness.py
cp $1/psl/embers-psl/test_publisher.py embers-psl/test_publisher.py 
cp -r $1/psl/psl-rss/src psl-rss/
cp -r $1/psl/psl-rss/aux_data psl-rss/
cp -r $1/psl/psl-rss/pom.xml psl-rss/pom.xml
cp -r $1/psl/psl-rss/start_PSL_geocoding.sh psl-rss/start_PSL_geocoding.sh
cp -r $1/psl/example_configs ./ 
