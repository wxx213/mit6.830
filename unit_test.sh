#!/bin/bash

set -e

PWD=`pwd`
LIB="$PWD/lib"

TEST="simpledb.systemtest.ScanTest"

if [ "$1" = "debug" ]; then
	JAVA="java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
else
	JAVA="java"
fi



ant clean
ant testcompile
$JAVA -cp .:$LIB/ant-contrib-1.0b3.jar:$LIB/hamcrest-core-1.3.jar:$LIB/jline-0.9.94.jar:$LIB/junit-4.13.1.jar:$LIB/zql.jar:$PWD/bin/src:$PWD/bin/test org.junit.runner.JUnitCore $TEST

