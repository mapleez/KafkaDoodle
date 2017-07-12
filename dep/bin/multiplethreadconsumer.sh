#!/bin/bash

BINDIR=`dirname $0`



if [ $# -lt 4 ]; then
	echo "ERROR arguments ($#): <bootstrap_servers> <topics> <groupid> <thread_num> [<SASLfile>]."
	exit 1;
fi

. $BINDIR/runclass.sh org.dt.ez.kafka.tools.complex.MultipleThreadKafkaConsumer "$@"

