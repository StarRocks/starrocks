# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

export HADOOP_USER_NAME=${USER}

# the purpose is to use local hadoop configuration first.
# under HADOOP_CONF_DIR(eg. /etc/ecm/hadoop-conf), there are hadoop/hdfs/hbase conf files.
# and by putting HADOOP_CONF_DIR at front of HADOOP_CLASSPATH, local hadoop conf file will be searched & used first.

# local hadoop configuration is usually well-tailored and optimized, we'd better to leverage that.
# for example, if local hdfs has enabled short-circuit read, then we can use short-circuit read and save io time

if [ ${HADOOP_CONF_DIR}"X" != "X" ]; then
    export HADOOP_CLASSPATH=${HADOOP_CONF_DIR}:${HADOOP_CLASSPATH}
fi

hadoop_jars=`find $STARROCKS_HOME/lib -name *.jar`
HADOOP_CLASSPATH=
for jar in $hadoop_jars
do
if [ ! -n "$HADOOP_CLASSPATH" ]; then
    HADOOP_CLASSPATH=$jar
else
    HADOOP_CLASSPATH=$jar:$HADOOP_CLASSPATH
fi
done
