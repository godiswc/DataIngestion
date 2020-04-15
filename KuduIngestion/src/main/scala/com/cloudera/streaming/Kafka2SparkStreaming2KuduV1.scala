package com.cloudera.streaming

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.cloudera.streaming.Kafka2SparkStreaming2KuduV1.delete
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.{ColumnSchema, Type}
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduSession, KuduTable, PartialRow, RowErrorsAndOverflowStatus}
import org.apache.kudu.consensus.Consensus
import org.apache.log4j.{Level, Logger}
import org.apache.kudu.spark.kudu._
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON


object Kafka2SparkStreaming2KuduV1 {
  val logger = LoggerFactory.getLogger(Kafka2SparkStreaming2KuduV1.getClass)
  Logger.getLogger("com").setLevel(Level.INFO) //设置日志级别
  //var confPath: String = System.getProperty("user.dir") + File.separator + "conf.properties"

  final val TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"
  final val PREFIX = "{\"owner\""

  /**
   * 建表Schema定义
   */
//  val userInfoSchema = StructType(
//    //         col name   type     nullable?
//      StructField("id", StringType , false) ::
//      StructField("name" , StringType, true ) ::
//      StructField("sex" , StringType, true ) ::
//      StructField("city" , StringType, true ) ::
//      StructField("occupation" , StringType, true ) ::
//      StructField("tel" , StringType, true ) ::
//      StructField("fixPhoneNum" , StringType, true ) ::
//      StructField("bankName" , StringType, true ) ::
//      StructField("address" , StringType, true ) ::
//      StructField("marriage" , StringType, true ) ::
//      StructField("childNum", StringType , true ) :: Nil
//  )



  /**
   * 定义一个UserInfo对象
   */
   case class UserInfo (
                        id: String,
                        name: String,
                        sex: String,
                        city: String,
                        occupation: String,
                        tel: String,
                        fixPhoneNum: String,
                        bankName: String,
                        address: String,
                        marriage: String,
                        childNum: String
                      )


  def main(args: Array[String]): Unit = {
    //加载配置文件
    val properties = new Properties()
    //System.out.println(SparkFiles.get("conf.properties"))
    //val file = new File(confPath)
    //if(!file.exists()) {
//      System.out.println(Kafka2SparkStreaming2Kudu.getClass.getClassLoader.getResource("conf.properties"))
//      val in = Kafka2SparkStreaming2Kudu.getClass.getClassLoader.getResourceAsStream("conf.properties")
//      properties.load(in);
    //} else {
     // properties.load(new FileInputStream(confPath))
    //}
//    System.out.println(this.getClass.getResource("conf.properties"))
//    val in = this.getClass.getResourceAsStream("conf.properties")
//    properties.load(in)
    properties.load(new FileInputStream("./conf.properties"))

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val kuduMaster = properties.getProperty("kudumaster.list")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)
    println("kudu.master:" + kuduMaster)

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(kuduMaster)) {
      println("未配置Kafka和KuduMaster信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet


    //val conf = new SparkConf().setMaster("local[4]").setAppName("Kafka2SparkStreaming2Kudu-kerberos");
    //val sc = new SparkContext(conf)

    //val spark = SparkSession.builder().appName("Kafka2SparkStreaming2Kudu-kerberos").master("local").getOrCreate()
    val spark = SparkSession.builder().appName("Kafka2SparkStreaming2Kudu-kerberos").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) //设置Spark时间窗口，每5s处理一次
    //val ssc = new StreamingContext(sc, Seconds(10))
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "earliest"
      , "security.protocol" -> "PLAINTEXT"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "testgroup1"
    )


    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //引入隐式
    import spark.implicits._
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    dStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        if(iter.hasNext){
          iter.foreach { record => {
            val value = record.value()
            if (value.startsWith(PREFIX)) {
              val jsonObj = JSON.parseFull(value)
              val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
              val tableName = map.get("tableName").get.asInstanceOf[String]
              val operationType = map.get("operationType").get.asInstanceOf[String]
              val afterColumnList = map.get("afterColumnList").get.asInstanceOf[Map[String, String]]
              //            val rdd = spark.sparkContext.parallelize(Seq(Row.fromSeq(afterColumnList.values.toSeq)))
              //            val schema = StructType(afterColumnList.keys.toSeq.map(StructField(_, StringType)))
              //            val df = spark.sqlContext.createDataFrame(rdd, schema)
              try {
                logger.info(s"Inserting data ${afterColumnList}")
                if (kuduContext.tableExists("impala::" + "ccic_kudu_sj_dev." + tableName)) {
                  //                if (operationType.equals("I")) {
                  //                  insert("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                  //                }
                  //                if (operationType.equals("U")) {
                  //                  insert("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                  //                }
                  //                if (operationType.equals("D")) {
                  //                  delete("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                  //                }
                  operationType match {
                    case "I" => insert("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                    case "U" => insert("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                    case "D" => delete("impala::" + "ccic_kudu_sj_dev." + tableName, afterColumnList, kuduContext)
                    case _ => logger.warn("Unknown OperationType")
                  }
                }
              } catch {
                case e: Exception => {
                  logger.error(s"Error parsing ${afterColumnList}", e)
                  //errorMsg = Option(e)
                }

              }
            }
          }
          }
        }
      })

      //将RDD转换为DataFrame
    //val userinfoDF = spark.sqlContext.createDataFrame(newrdd)
    //kuduContext.upsertRows(userinfoDF, "user_info")
    //kuduContext.in
  })
    ssc.start()
    ssc.awaitTermination()
  }



  def insert(tableName: String, columnList: Map[String,String], context: KuduContext): Unit = {
    //val syncClient = context.syncClient
    val lastPropagatedTimestamp = context.syncClient.getLastPropagatedTimestamp
    context.syncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp)
    val table = context.syncClient.openTable(tableName)
    val session = context.syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    //session.setIgnoreAllDuplicateRows(true)

    val schema = table.getSchema
    val insert = table.newUpsert()
    val row = insert.getRow
    columnList.foreach{ case (column, newValue) => {
      try {
        addColumnData(row, schema.getColumn(column.toLowerCase), newValue)
      } catch {
        case e: IllegalArgumentException => {
          logger.warn(s"Will not add ($column, $newValue) for ${table.getName}", e)
        }
      }
      //      logger.info(s"Writing $column , $newValue")
    }}
    session.apply(insert)
    session.close()
    context.timestampAccumulator.add(context.syncClient.getLastPropagatedTimestamp)

  }


  def delete(tableName: String, columnList: Map[String,String], context: KuduContext): Unit = {
    //val syncClient = context.syncClient
    val lastPropagatedTimestamp = context.syncClient.getLastPropagatedTimestamp
    context.syncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp)
    val table = context.syncClient.openTable(tableName)
    val session = context.syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    //session.setIgnoreAllDuplicateRows(true)

    val schema = table.getSchema

    val delete = table.newDelete()
    val row = delete.getRow
    columnList.foreach{ case (column, value) => {
      if (schema.getColumn(column.toLowerCase()).isKey) {
        try {
          addColumnData(row, schema.getColumn(column.toLowerCase()), value)
        } catch {
          case e: IllegalArgumentException => {
            logger.warn(s"Will not add ($column, $value) for ${table.getName}", e)
          }
        }
      }
    }}
    session.apply(delete)
    session.close()
    context.timestampAccumulator.add(context.syncClient.getLastPropagatedTimestamp)
  }

  def addColumnData(row: PartialRow, column: ColumnSchema, newValue: String) : Unit = {
    if (newValue != null && !newValue.equals("")) {
      column.getType() match {
        case Type.INT8 => row.addByte(column.getName, newValue.toByte)
        case Type.INT16 => row.addShort(column.getName, newValue.toShort)
        case Type.INT32 => row.addInt(column.getName, newValue.toInt)
        case Type.INT64 => row.addLong(column.getName, toDateOrElseBigInt(newValue))
        case Type.BINARY => row.addBinary(column.getName, newValue.getBytes())
        case Type.STRING => row.addString(column.getName, newValue)
        case Type.BOOL => row.addBoolean(column.getName, newValue.toBoolean)
        case Type.FLOAT => row.addFloat(column.getName, newValue.toFloat)
        case Type.DOUBLE => row.addDouble(column.getName, newValue.toDouble)
        case _ => throw new UnsupportedOperationException("Unknown type " + column.getName)
      }
    } else {
      row.setNull(column.getName)
    }
  }

  /**
   * Parse "yyyy-MM-dd hh:mm:ss" formatted string to timestamp.
   * If in date format, then just parse to long value.
   * The input form is
   * @param dateString
   * @return
   */
  def toDateOrElseBigInt(dateString: String): Long = {
    val formatter = new SimpleDateFormat(TIMESTAMP_FORMAT)

    val test = Try[Date](formatter.parse(dateString))
    test match {
      case Success(date) => {
        // ms to s
        date.getTime / 1000
      }
      case Failure(exception) => dateString.toLong
    }
  }
}
