#!/bin/bash

set -e

PWD=`pwd`
LIB="$PWD/lib"



TEST1="simpledb.TupleTest simpledb.TupleDescTest simpledb.CatalogTest simpledb.RecordIdTest \
	simpledb.HeapPageReadTest simpledb.HeapPageIdTest simpledb.HeapFileReadTest"
TESTS="simpledb.systemtest.ScanTest $TEST1"

if [ "$1" = "debug" ]; then
	JAVA="java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
else
	JAVA="java"
fi



ant clean
ant testcompile
for TEST in $TESTS; do
	$JAVA -cp .:$LIB/ant-contrib-1.0b3.jar:$LIB/hamcrest-core-1.3.jar:$LIB/jline-0.9.94.jar:$LIB/junit-4.13.1.jar:$LIB/zql.jar:$PWD/bin/src:$PWD/bin/test org.junit.runner.JUnitCore $TEST
done
