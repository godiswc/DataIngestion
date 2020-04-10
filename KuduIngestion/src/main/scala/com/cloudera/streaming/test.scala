package com.cloudera.streaming

import com.cloudera.streaming.Kafka2SparkStreaming2Kudu.{addColumnData, logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.parsing.json.JSON

object test {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Kafka2-kerberos").master("local").getOrCreate()

    //val myMap = Map("Col_1"->"1", "Col_2"->"2", "Col_3"->"3")
    val myMap = Map("ARGUE_SOLUTION_TYPE" -> " 01", " HISTORY_POLICY_NO " -> " PZBM201834011804000023", " BASE_PREMIUM " -> " ", " MASTER_POLICY_ID " -> " ", " IS_SPECIAL_BUSINESS " -> " N", " VAT " -> " 220.75", " PRODUCT_ID " -> " 22222222222", " REGULAR_SETTLEMENT_MODE_CODE " -> " ", " START_REVISION_ID " -> " 7900000000000038", " IS_SAVE_PLAN_DETAIL " -> " N", " IS_LARGE_GROUP_POLICY " -> " ", " ASSOCIATED_PROPOSAL_NO " -> " ", " AUDIT_OPINION " -> " ", " POLICY_PRINT_TYPE " -> " 3", " PLAN_CODE " -> " ", " BUSINESS_CATE " -> " 1", " IS_GOVERNMENT_BIZ " -> " ", " SI_CURRENCY_CODE " -> " CNY", " POLICY_HOLDER_CUSTOMER_CODE " -> " 90017412086", " TEAM_CODE " -> " ", " NATIONAL_ECONOMY_CATE " -> " ", " SUM_INSURED_LOCAL " -> " 3400000", " LOCAL_CURRENCY_CODE " -> " CNY", " POLICY_STATUS " -> " 2", " IS_SPECIAL_GROUP " -> " ", " GROUP_BUSINESS_CODE " -> " 02", " ISSUE_ORG_CODE " -> " 34011805", " PRD_PKG_DISCOUNT_RATE " -> " ", " COMMIT_TIMESTAMP " -> " 2019-03-28 14:25:55", " BELONG_TO_HANDLER_ID " -> " 26004873143", " POLICY_NO " -> " PZBM19340118050000000018", " C1_FEE_RATE " -> " ", " PREMIUM_LOCAL_EXCHANGE_RATE " -> " 1", " REGULAR_ENDO_DAYS_COUNT " -> " ", " UNDERWRITING_DATE " -> " 2019-02-19 11:36:57", " AGREEMENT_NO " -> " U10000001562-2018006", " PROPOSAL_NO " -> " TZBM19340118050000000019", " CHANNEL_CODE_BY_PERSON " -> " C0000016", " ASSOCIATED_POLICY_NO " -> " ", " BUSINESS_SOURCE_CODE " -> " 2101", " STD_PREMIUM_LOCAL " -> " ", " UPDATE_BY " -> " 260064029580813", " IS_LARGE_BUSINESS_RISK " -> " ", " SUM_INSURED " -> " 3400000", " INTERNAL_CO_INSURANCE_TYPE " -> " 01", " IS_REGULAR_SETTLEMENT " -> " ", " SI_LOCAL_EXCHANGE_RATE " -> " 1", " ARBITRATION_COMMISSION_CODE " -> " ", " POLICY_ID0 " -> " 3003664443882041", " BEFORE_VAT_PREMIUM_LOCAL " -> " 3679.25", " COMPY_VEH_TAX_PREMIUM " -> " ", " SMS_SEND_CUST_ROLE_CODE " -> " ", " IS_RENEWABLE " -> " Y", " IS_LOAD_VISA_LETTER " -> " ", " BASE_PREMIUM_LOCAL " -> " ", " IS_SHORT_TERM " -> " ", " ADJUSTED_PREMIUM " -> " 3900", " DML_TYPE " -> " U", " APP_ID " -> " ", " AGENCY_LICENSE_NO " -> " ", " HANDLER_LICENSE_NO " -> " ", " CERTIFICATE_PAYMENT_TYPE " -> " ", " IS_PACKAGE_PRODUCT " -> " N", " IS_AUTO_UW " -> " N", " TOTAL_DISCOUNT_RATE " -> " ", " SYNC_TIMESTAMP " -> " 2019-03-28 14:26:03", " UPDATE_TIME " -> " 2019-03-20 09:12:49", " GROSS_PREMIUM_LOCAL " -> " 3899.97", " HANDLER_NAME " -> " 安徽皖运保险代理有限公司", " IS_ISSUE_AFTER_PAY " -> " N", " SHORT_RATE_TYPE " -> " 02", " BUSINESS_ATTRIBUTE " -> " 0000", " CO_INSURANCE_TYPE " -> " 01", " PRD_PKG_DISCOUNT " -> " ", " TERMINATE_DATE " -> " ", " RENEWAL_TYPE " -> " 1", " C2_FEE_RATE " -> " ", " DC_POLICY_NO " -> " ", " AGENT_NAME " -> " 安徽皖运保险代理有限公司", " JUDICAL_SCOPE_TEXT " -> " ", " BOOK_CURRENCY " -> " CNY", " IS_SMALL_AMOUNT " -> " ", " RATING_METHOD_CODE " -> " ", " INSURED_CUSTOMER_CODE " -> " 90017412086", " ASSOCIATED_INSURER_CODE " -> " ", " AGRICULTURE_RELA_TYPE " -> " N", " IS_SEND_SMS " -> " ", " EXPECTED_ADJUSTED_RATE " -> " ", " IS_REGULAR_ENDO " -> " ", " STD_PREMIUM " -> " ", " ADJUSTED_PREMIUM_LOCAL " -> " ", " DUE_PREMIUM_LOCAL " -> " 3900", " AGRICULTURE_NATURE_CODE " -> " ", " BELONG_TO_HANDLER_TEL " -> " ", " BELONG_TO_HANDLER_NAME " -> " 黄峰", " BUSINESS_SOURCE2_CODE " -> " 2101", " POLICY_TYPE " -> " 1", " BELONG_TO_HANDLER2_CODE " -> " U10000001562", " PREMIUM_BOOK_EXCHANGE_RATE " -> " 1", " IS_INSTALLMENT " -> " ", " IS_HAVE_DETAIL_COVERAGE " -> " ", " INSURED_LIST_TYPE " -> " ", " POLICY_ID " -> " 9900000000000008", " DUE_PREMIUM " -> " 3900", " ISSUE_USER_ID " -> " 26004917171", " REVISION_TYPE " -> " ", " REPAIR_CHANNEL_CODE " -> " ", " PROPOSAL_DATE " -> " 2019-02-19 09:28:04", " BELONG_TO_HANDLER_CODE " -> " 8000068611", " BELONG_TO_HANDLER2_NAME " -> " 安徽皖运保险代理有限公司", " SPECIAL_FLAG_CODE " -> " ", " IS_COMPLETE_E_PROPOSAL " -> " ", " PREMIUM_CURRENCY_CODE " -> " CNY", " AGENT_ID " -> " 26006983857", " NATIONAL_ECONOMY_L2_CATE " -> " ", " IS_TAKE_UP_POLICY " -> " N", " NATIONAL_ECONOMY_L1_CATE " -> " ", " IS_PUSH_MEDICAL_INSURANCE " -> " ", " NATIONAL_ECONOMY_L3_CATE " -> " ", " PROJECT_NO " -> " ", " GROSS_PREMIUM " -> " 3899.97", " APP_NO " -> " ", " ARBITRATION_COMMISSION_TEXT " -> " ", " ORG_ID " -> " 260060047158204", " AGREEMENT_NAME " -> " 2018-2021年安徽皖运保险代理有限公司保险代理合同", " LATEST_REGULAR_SETTLE_DATE " -> " ", " OVERSEA_BUSINESS_TYPE " -> " 1", " VAT_RATE " -> " ", " INSERT_TIME " -> " 2019-02-19 11:36:58", " ISSUE_ADDRESS " -> " ", " JUDICAL_SCOPE_CODE " -> " 01", " VAT_LOCAL " -> " 220.75", " EXPIRY_DATE " -> " 2020-04-02 23:59:59", " REGULAR_SETTLEMENT_DATE " -> " ", " EXPECTED_PREMIUM " -> " ", " THIRD_PARTY_AGENT_CODE " -> " ", " SHORT_RATE " -> " 1", " NATIONAL_ECONOMY_L4_CATE " -> " ", " IS_MASTER_POLICY " -> " ", " POLICY_FROM_TYPE " -> " ", " ID_CHECK_RESULT_CODE " -> " ", " REGION_CODE " -> " 340000", " INSERT_BY " -> " 26004917171", " AGENT_LICENSE_NO " -> " ", " IS_RI_IN " -> " ", " PRODUCT_CODE " -> " ZBM", " SMS_TEMPLATE_ID " -> " ", " ORG_CODE " -> " 34011851", " QUOTATION_NO " -> " ", " AGENT_CODE " -> " U10000001562", " MASTER_POLICY_NO " -> " ", " IS_FROM_DC " -> " ", " ELECTRONIC_POLICY_TEMPLATE_ID " -> " ", " TERMINATE_CODE " -> " ", " C_FEE_RATE " -> " ", " IS_SEND_E_POLICY " -> " N", " IS_E_PROPOSAL " -> " ", " BEFORE_VAT_PREMIUM " -> " 3679.25", " POLICY_HOLDER_NAME " -> " 安徽交运集团滁州汽运有限公司", " EFFECTIVE_DATE " -> " 2019-04-03 00:00:00", " MP_TOTAL_PREPAY_PREMIUM " -> " ", " CESSION_BASIS_CODE " -> " ", " IS_BID " -> " ", " HANDLER_CODE " -> " U10000001562", " PREM_CALC_HASH " -> " ", " ISSUE_ORG_ID " -> " 260059653500187", " SUBMISSION_VER_ID " -> " ", " MP_TOTAL_SUM_INSURED " -> " ", " VERSION_SEQ " -> " 3", " INSD_LIAB_POLICY_NO " -> " ", " INSURED_NAME " -> " 安徽交运集团滁州汽运有限公司直属分公司", " POLICY_NATURE " -> " 01", "SUBMISSION_ID " -> "")
    //val rdd = spark.sparkContext.parallelize(Seq(Row.fromSeq(myMap.values.toSeq)))
    //val schema = StructType(myMap.keys.toSeq.map(StructField(_, StringType)))
    //rdd.collect().foreach(println);
    val s = "222222"
    println(s.toInt)
    println(myMap)
    myMap.foreach { case (column, newValue) => {
      try {
        //addColumnData(row, schema.getColumn(column.toLowerCase), newValue)
        println(column)
        println(newValue)
      } catch {
        case e: IllegalArgumentException => {
          //logger.warn(s"Will not add ($column, $newValue) for ${table.getName}", e)
        }
      }
    }
    }
      //      logger.info(s"Writing $column , $newValue"

      //
      //    val collected = rdd.map(record => {
      //      val json = JSON.parseFull(record.value())
      //      val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
      //      val tableName = map.get("tableName").get.asInstanceOf[String]
      //      val operationType = map.get("operationType").get.asInstanceOf[String]
      //      val afterColumnList =  map.get("afterColumnList").get.asInstanceOf[Map[String, Any]]
      //      //(tableName,operationType,afterColumnList)
      //      (tableName,operationType,afterColumnList)
      //      //spark.sparkContext
      //      spark.sparkContext.parallelize("1")
      //      //val rdd = spark.sparkContext.parallelize(Seq(Row.fromSeq(afterColumnList.values.toSeq)))
      //      //val schema = StructType(afterColumnList.keys.toSeq.map(StructField(_, StringType)))
      //      //val df = spark.sqlContext.createDataFrame(rdd, schema)
      //      //df
      //
      //      kuduContext.asyncClient.s
      //
      //      afterColumnList
      //    }).collect()
      //    for ( c <- collected )  {
      //      println(c)
      //    }
      //
      //
      //
      //
      //    rdd.map(line => {
      //      val jsonObj =  JSON.parseFull(line.value())
      //      val map:Map[String,Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
      //      val tableName = map.get("tableName").get.asInstanceOf[String]
      //      val operationType = map.get("operationType").get.asInstanceOf[String]
      //      val afterColumnList =  map.get("afterColumnList").get.asInstanceOf[Map[String, Any]]
      //
      //      val rdd = spark.sparkContext.parallelize(Seq(Row.fromSeq(afterColumnList.values.toSeq)))
      //      val schema = StructType(afterColumnList.keys.toSeq.map(StructField(_, StringType)))
      //      val df = spark.sqlContext.createDataFrame(rdd, schema)
      //
      //      if(kuduContext.tableExists("impala::"+"ccic_kudu_sj_dev."+tableName)) {
      //        if (operationType.equals("I")) {
      //          kuduContext.upsertRows(df, "impala::" + "ccic_kudu_sj_dev." + tableName)
      //        }
      //        if (operationType.equals("U")) {
      //          kuduContext.updateRows(df, "impala::" + "ccic_kudu_sj_dev." + tableName)
      //        }
      //        if (operationType.equals("D")) {
      //          kuduContext.deleteRows(df, "impala::" + "ccic_kudu_sj_dev." + tableName)
      //        }
      //      }

    }
  }
