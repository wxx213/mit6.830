#!/bin/bash

set -e

#ant runtest -Dtest=HeapPageIdTest
#ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapPageReadTest
