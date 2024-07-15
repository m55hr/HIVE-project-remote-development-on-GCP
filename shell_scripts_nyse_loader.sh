NAMENODE=${1}
HDFS_SCRIPT_DIR=${2}
USERNAME=${3}
hive -f hdfs://${NAMENODE}/${HDFS_SCRIPT_DIR}/nyse_hdfs_loader.hql --hivevar username=${USERNAME} --verbose