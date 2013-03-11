#!/bin/sh

JASPERHOME=.

cd $JASPERHOME/bin
java -cp .:../lib/logging/slf4j-api-1.6.1.jar:../lib/logging/logback-core-0.9.27.jar:../lib/logging/logback-classic-0.9.27.jar:../lib/codecs/twitter4j-core-2.1.10.jar:../lib/netty-3.2.3.Final.jar:../lib/jgrapht-jdk1.6.jar edu/jhu/cs/damsl/utils/hw2/HW2Terminal
