#!/bin/bash

BINDIR=`dirname $0`
ROOTDIR=$BINDIR/..

LIBDIR=$ROOTDIR/lib
LOGDIR=$ROOTDIR/logs
CONFDIR=$ROOTDIR/conf

CP=.
MAINCLASS=com.joinbright.kafka.tools.

for jar in $LIBDIR/*.jar; do
	CP=$jar:$CP
done

CP=$CP:$CONFDIR

while [ $# -gt 0 ]; do
	COMMAND=$1
	case $COMMAND in
		-tool)
			MAINCLASS=${MAINCLASS}$2
			shift 2
			;;
		*)
			break
			;;
	esac
done

java -cp $CP $MAINCLASS

