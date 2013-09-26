#!/bin/bash
mvn compile

mvn dependency:build-classpath -Dmdep.outputFile=classpath.out

java -Xmx1g -cp ./target/classes:`cat classpath.out` edu.umd.cs.linqs.embers.PSLRewriterTrainer $1 $2