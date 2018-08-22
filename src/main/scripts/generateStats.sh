#!/bin/sh

#####################################################################################
# This script runs the spark job to consume Span events and generate stats          #
#####################################################################################

export FROM_EMAIL="kmiry@allstate.com"
export ALERT_EMAIL="kmiry@allstate.com"
#export ALERT_EMAIL="hadoopcoe@allstate.com"
export LOG_DIR=/allstate/log
export VISION_HOME="/home/kmiry/vision_10"

# Ensure that JDK1.8 is used!
export PATH=/usr/java/latest/bin:$PATH

processDate=`date '+%Y%m%d%H%M%S'`

# Define the functions
log_and_alert() {
    echo "`date +'%m-%d-%Y %H:%M:%S'`:  $1" >> ${LOG_DIR}/vision_stats_${processDate}.log
    echo -e "`date +'%m-%d-%Y %H:%M:%S'`:\n  $1" | mailx -s "$2" -r "${FROM_EMAIL}" "${ALERT_EMAIL}"
}

log() {
    echo "`date +'%m-%d-%Y %H:%M:%S'`:  $1" >> ${LOG_DIR}/vision_stats_${processDate}.log
}

export JARS_DIR="${VISION_HOME}/jars"
export VISION_CONF_FILE="${VISION_HOME}/conf/vision.conf"
export KEYTAB_FILE="/home/kmiry/kmiry.keytab"
export HDFS_PRINCIPAL="kmiry@AD.ALLSTATE.COM"
export JAAS_CONF="/home/kmiry/kerberos/rtalab_vision/rtalab_vision.jaas"
export KRB5_CONF="/home/kmiry/kerberos/krb5.conf"
export LOG4J_CONF="file:///home/kmiry/vision/conf/log4j.properties"
export SPARK_KAFKA_VERSION=0.10

log "Running spark job ...."

spark2-submit \
    --keytab ${KEYTAB_FILE} \
    --principal ${HDFS_PRINCIPAL} \
    --driver-java-options "-Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=${JAAS_CONF} -Djava.security.krb5.conf=${KRB5_CONF} -Djavax.security.auth.useSubjectCredsOnly=false" \
    --class com.allstate.bigdatacoe.vision.stats.GenerateFactSpanStats \
    --conf spark.ui.port=5886 \
    /home/kmiry/vision/VisionAnomalyDetection-assembly-1.1.jar >> ${LOG_DIR}/vision_stats_test10_${processDate}.log 2>&1

returnCode=$?
if [ ${returnCode} == 101 ]
then
    log_and_alert "No new span events available to ingest from vision." "No new events"
fi

if [ ${returnCode} != 0 ] && [ ${returnCode} != 101 ]
then
    log_and_alert "Error occurred while generating stats for Vision span data. Please refer to log file ${LOG_DIR}/vision_stats_${processDate}.log." "Error generating stats for Vision span data."
    exit 1
fi
log "Generated stats for Vision Span data."

log "Deleting log files older than 15 days..."
find ${LOG_DIR} -name "vision_stats_*" -mtime +15 -exec rm -f {} \; 2>>${LOG_DIR}/vision_stats_${processDate}.log

log "Deleting offset files older than 15 days..."
offsetDir=`grep vision.kafka.factSpanOffsetsDir ${VISION_CONF_FILE} | cut -d"=" -f2 | sed -e 's/file:\/\///g' -e 's/ //g'`
find ${offsetDir} -type f -name "*.json_2*" -mtime +15 -exec rm -f {} \; 2>>${LOG_DIR}/vision_stats_${processDate}.log

log "Successfully generated stats for Vision Span data"