# install 1.2
mvn install:install-file -Dfile=psl-1.2-SNAPSHOT/psl-1.2-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl -Dversion=1.2-SNAPSHOT -Dpackaging=pom
mvn install:install-file -Dfile=psl-1.2-SNAPSHOT/psl-core-1.2-SNAPSHOT.jar -DpomFile=psl-1.2-SNAPSHOT/psl-core-1.2-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl-core -Dversion=1.2-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=psl-1.2-SNAPSHOT/psl-groovy-1.2-SNAPSHOT.jar -DpomFile=psl-1.2-SNAPSHOT/psl-groovy-1.2-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl-groovy -Dversion=1.2-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=psl-1.2-SNAPSHOT/psl-parser-1.2-SNAPSHOT.jar -DpomFile=psl-1.2-SNAPSHOT/psl-parser-1.2-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl-parser -Dversion=1.2-SNAPSHOT -Dpackaging=jar
