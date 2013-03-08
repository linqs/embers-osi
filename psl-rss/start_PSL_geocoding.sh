#!/bin/bash
mvn compile
mvn dependency:build-classpath -Dmdep.outputFile=classpath.out

java -cp ./target/classes:`cat classpath.out` edu.umd.cs.linqs.embers.PrepareRSSAnalysis
java -cp ./target/classes:`cat classpath.out` edu.umd.cs.linqs.embers.JSONServer

