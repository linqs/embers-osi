mvn install:install-file -Dfile=psl-1.1-SNAPSHOT/psl-1.1-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl -Dversion=1.1-SNAPSHOT -Dpackaging=pom
mvn install:install-file -Dfile=psl-1.1-SNAPSHOT/psl-core-1.1-SNAPSHOT.jar -DpomFile=psl-1.1-SNAPSHOT/psl-core-1.1-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl-core -Dversion=1.1-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=psl-1.1-SNAPSHOT/psl-groovy-1.1-SNAPSHOT.jar -DpomFile=psl-1.1-SNAPSHOT/psl-groovy-1.1-SNAPSHOT.pom.xml -DgroupId=edu.umd.cs -DartifactId=psl-groovy -Dversion=1.1-SNAPSHOT -Dpackaging=jar
