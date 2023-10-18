hdfs dfs -mkdir -p /data/openbeer/data/input
hdfs dfs -put /shared_data/bigmac.csv /data/openbeer/data/input/bigmac.csv
hdfs dfs -put /shared_data/inflation.csv /data/openbeer/data/input/inflation.csv
hdfs dfs -put  /shared_data/world-data-2023.csv /data/openbeer/data/input/world-data-2023.csv
hdfs dfs -put  /shared_data/mcdo.csv /data/openbeer/data/input/mcdo.csv