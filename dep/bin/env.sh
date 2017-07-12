#!/bin/bash

BINDIR=`dirname $0`
ROOTDIR=$BINDIR/..

LIBDIR=$ROOTDIR/lib
LOGDIR=$ROOTDIR/logs
LOGFILE=$(basename $0 .sh)".log"
CONFDIR=$ROOTDIR/conf

CP=.
JVM_OPTS="`cat $CONFDIR/jvm.conf | xargs`"

for jar in $LIBDIR/*.jar; do
		CP=$jar:$CP
done

CP=$CP:$CONFDIR

