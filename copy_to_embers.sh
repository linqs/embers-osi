
echo copying to $1

mkdir $1/psl/
mkdir $1/psl/psl-rss
mkdir $1/psl/psl-rss/target
mkdir $1/psl/psl-rewriter
mkdir $1/psl/psl-rewriter/target
mkdir $1/psl/example_configs


cp README.md $1/psl/README.md
cp install_psl.sh $1/psl/install_psl.sh
cp -r psl-1.2-SNAPSHOT/ $1/psl/psl-1.2-SNAPSHOT
cp -r psl-rss/src $1/psl/psl-rss/
cp -r psl-rss/aux_data $1/psl/psl-rss/
cp -r psl-rss/pom.xml $1/psl/psl-rss/pom.xml
cp -r psl-rss/start_PSL_geocoding.sh $1/psl/psl-rss/start_PSL_geocoding.sh
cp -r example_configs $1/psl/

cp -r psl-rewriter/src $1/psl/psl-rewriter/
cp -r psl-rewriter/pom.xml $1/psl/psl-rewriter/pom.xml
cp -r psl-rewriter/model.psl $1/psl/psl-rewriter/

rm $1/psl/psl-rss/aux_data/rss-content-enriched-2012-12-03-12-36-41.txt

cd psl-rss
mvn compile
mvn install
cd ..
cp ~/.m2/repository/edu/umd/cs/linqs/embers/psl-rss/1.0-SNAPSHOT/psl-rss-1.0-SNAPSHOT.jar ./psl-rss/target
cp psl-rss/target/psl-rss-1.0-SNAPSHOT.jar $1/psl/psl-rss/target/

cd psl-rewriter
mvn compile
mvn install
cd ..
cp ~/.m2/repository/edu/umd/cs/linqs/embers/psl-rewriter/1.0-SNAPSHOT/psl-rewriter-1.0-SNAPSHOT.jar ./psl-rewriter/target
cp psl-rewriter/target/psl-rewriter-1.0-SNAPSHOT.jar $1/psl/psl-rewriter/target/

