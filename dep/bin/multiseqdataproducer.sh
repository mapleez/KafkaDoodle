#!/bin/bash

BINDIR=`dirname $0`


if [ $# -lt 3 ]; then
	echo "ERROR arguments ($#): <bootstrap_servers> <topics> <thread_num> [<SASLfile>]."
	exit 1;
fi

. $BINDIR/runclass.sh org.dt.ez.kafka.doodle.partitions.MultiSeqDataProducer "$@"

