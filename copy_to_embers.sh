
echo copying to $1

cp README.md $1/psl/README.md
cp install_psl.sh $1/psl/install_psl.sh
cp -r psl-1.1-SNAPSHOT/ $1/psl/psl-1.1-SNAPSHOT
cp embers-psl/psl_harness.py $1/psl/embers-psl/psl_harness.py
cp embers-psl/test_publisher.py $1/psl/embers-psl/test_publisher.py
cp -r psl-rss/src $1/psl/psl-rss/
cp -r psl-rss/aux_data $1/psl/psl-rss/
cp -r psl-rss/pom.xml $1/psl/psl-rss/pom.xml
cp -r psl-rss/start_PSL_geocoding.sh $1/psl/psl-rss/start_PSL_geocoding.sh
cp -r example_configs $1/psl/
cp psl-rss/target/psl-rss-1.0-SNAPSHOT.jar $1/psl/psl-rss/target/