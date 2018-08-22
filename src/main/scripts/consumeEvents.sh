#!/bin/sh

###########################################################################################
# This script runs the spark job to consume Span events and categorize the alert level    #
###########################################################################################

export FROM_EMAIL="kmiry@allstate.com"
export ALERT_EMAIL="kmiry@allstate.com"
#export ALERT_EMAIL="hadoopcoe@allstate.com"
export LOG_DIR=/allstate/log
export VISION_HOME="/home/kmiry/vision_10"

processDate=`date '+%Y%m%d%H%M%S'`

# Define the functions
log_and_alert() {
    echo "`date +'%m-%d-%Y %H:%M:%S'`:  $1" >> ${LOG_DIR}/vision_span_${processDate}.log
    echo -e "`date +'%m-%d-%Y %H:%M:%S'`:\n  $1" | mailx -s "$2" -r "${FROM_EMAIL}" "${ALERT_EMAIL}"
}

log() {
    echo "`date +'%m-%d-%Y %H:%M:%S'`:  $1" >> ${LOG_DIR}/vision_span_${processDate}.log
}

export JARS_DIR="${VISION_HOME}/jars"
export SPARK_KAFKA_VERSION=0.10
export VISION_CONF_FILE="${VISION_HOME}/conf/vision.conf"
export KEYTAB_FILE="/home/kmiry/kmiry.keytab"
export HDFS_PRINCIPAL="kmiry@AD.ALLSTATE.COM"
export JAAS_CONF="/home/kmiry/kerberos/rtalab_vision/rtalab_vision.jaas"
export KRB5_CONF="/home/kmiry/kerberos/krb5.conf"
export LOG4J_CONF="file:///home/kmiry/vision/conf/log4j.properties"


#kinit -k -t ${KEYTAB_FILE} ${HDFS_PRINCIPAL}
#if [ $? != 0 ]
#then
    # Failed to get a ticket. Log accordingly and exit!
#    log "No valid Kerberos ticket available."
#    exit 1
#fi
#log "Got the kerberos key."

log "Running job to consume Vision span events... "
echo "Submiting...."
# TODO: add security parameters to extra classpath for executor and driver
spark2-submit \
    --keytab /home/kmiry/kmiry.keytab \
    --principal ${HDFS_PRINCIPAL} \
    --jars /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10/spark-streaming-kafka-0-10_2.11-2.3.0.cloudera2.jar,/opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10/kafka-clients-0.10.0-kafka-2.1.0.jar \
    --driver-java-options "-Dlog4j.configuration=${LOG4J_CONF} -Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=${LOG4J_CONF} -Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --conf "spark.dynamicAllocation.executorIdleTimeout=30s" \
    --conf "spark.dynamicAllocation.minExecutors=10" \
    --class com.allstate.bigdatacoe.vision.streaming.ConsumeFactSpanEvents \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.ui.port=6669 \
    --conf spark.yarn.maxAppAttempts=4 \
    --conf spark.yarn.max.executor.failures=100 \
    --conf spark.task.maxFailures=8 \
    /home/kmiry/vision/VisionAnomalyDetection-assembly-1.1.jar >> ${LOG_DIR}/vision_span_test10_${processDate}.log 2>&1 &

if [ $? != 0  ]
then
    log_and_alert "Error occurred while consumingVision span data. Please refer to log file." "Error while consuming Vision span data."
    exit 1
fi
echo "Exiting script."
