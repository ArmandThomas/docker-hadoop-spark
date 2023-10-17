#!/bin/bash

set -x  # Activation du mode de débogage

namedir=`echo $HDFS_CONF_dfs_namenode_name_dir | perl -pe 's#file://##'`

echo "Copied world-data-2023.csv"

if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

if [ -z "$CLUSTER_NAME" ]; then
  echo "Cluster name not specified"
  exit 2
fi

echo "remove lost+found from $namedir"
rm -r $namedir/lost+found

if [ "`ls -A $namedir`" == "" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format $CLUSTER_NAME
fi

#$HADOOP_HOME/bin/
# hdfs dfs -mkdir -p /data/openbeer/data/input
#$HADOOP_HOME/bin/
# hdfs dfs -put /shared_data/bigmac.csv /data/openbeer/data/input/bigmac.csv
#$HADOOP_HOME/bin/
# hdfs dfs -put /shared_data/inflation.csv /data/openbeer/data/input/inflation.csv
#$HADOOP_HOME/bin/
# hdfs dfs -put  /shared_data/world-data-2023.csv /data/openbeer/data/input/world-data-2023.csv

$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode

# Create HDFS directories




