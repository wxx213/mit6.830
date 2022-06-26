#!/bin/bash

set -e

PWD=`pwd`
LIB="$PWD/lib"



LAB1_TESTS="simpledb.TupleTest simpledb.TupleDescTest simpledb.CatalogTest simpledb.RecordIdTest simpledb.HeapPageReadTest simpledb.HeapPageIdTest simpledb.HeapFileReadTest simpledb.systemtest.ScanTest"

LAB2_TESTS="simpledb.PredicateTest simpledb.JoinPredicateTest simpledb.FilterTest simpledb.JoinTest simpledb.systemtest.FilterTest simpledb.systemtest.JoinTest simpledb.IntegerAggregatorTest simpledb.StringAggregatorTest simpledb.AggregateTest simpledb.systemtest.AggregateTest simpledb.HeapPageWriteTest simpledb.HeapFileWriteTest simpledb.BufferPoolWriteTest simpledb.systemtest.EvictionTest"

LAB3_TESTS="simpledb.IntHistogramTest simpledb.TableStatsTest simpledb.JoinOptimizerTest simpledb.systemtest.QueryTest"

LAB4_TESTS="simpledb.LockingTest simpledb.TransactionTest simpledb.systemtest.AbortEvictionTest"

TESTS="$LAB1_TESTS $LAB2_TESTS $LAB3_TESTS $LAB4_TESTS"

TESTS="simpledb.TransactionTest simpledb.systemtest.AbortEvictionTest"

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
