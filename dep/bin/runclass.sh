#!/bin/bash

BINDIR=`dirname $0`

. $BINDIR/env.sh

MAINCLASS=$1
shift

JAVA=java

if [ "x$MAINCLASS" == "x" ]; then
	echo "Specified main class name!";
	exit 1;
fi

echo $LOGFILE

nohup $JAVA -cp $CP $JVM_OPTS $MAINCLASS "$@" >> $LOGDIR/$LOGFILE 2>&1 &

