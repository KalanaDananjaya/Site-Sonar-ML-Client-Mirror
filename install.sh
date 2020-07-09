#!/bin/bash

export PATH=$PATH:/sbin:/usr/sbin:/usr/local/sbin

java=`which java | grep -v -E -e "^which: no"`

while [ -z "${java}" ]; do
    echo -n "Path to java home directory: "
    read jt
    if [ -x "${jt}/bin/java" ]; then
	echo "The path you specified seems to be ok."
	java="${jt}/bin/java"
    else
	echo "No java executable was found in the path you specified. Please do not include /bin or /bin/java in the path."
    fi
done

java=`dirname "${java}"`
java=`dirname "${java}"`

cd `dirname $0`

echo ${java} > conf/env.JAVA_HOME || exit

chmod a+x run.sh update.sh
chmod a-x install.sh
