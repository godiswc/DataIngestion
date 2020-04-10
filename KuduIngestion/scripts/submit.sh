export SPARK_KAFKA_VERSION=0.10
spark2-submit --class com.cloudera.streaming.Kafka2SparkStreaming2Kudu \
    --master yarn \
    --deploy-mode client \
    --executor-memory 4g \
    --executor-cores 4 \
    --driver-memory 2g \
    --num-executors 2 \
    --queue default  \
    --principal app@CDH.COM \
    --keytab /root/DataIngestion/app.keytab \
    --jars /opt/cloudera/parcels/SPARK2/lib/spark2/kafka-0.10/spark-streaming-kafka-0-10_2.11-2.4.0.cloudera2.jar \
    KuduIngestion-1.0-SNAPSHOT.jar
