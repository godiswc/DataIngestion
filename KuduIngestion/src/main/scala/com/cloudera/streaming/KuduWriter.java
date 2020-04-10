package com.cloudera.streaming;

import org.apache.kudu.client.KuduTable;

public class KuduWriter {

    //final val TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"

//    def insert(table:KuduTable, cols: Map[String, String]): Unit ={
////    val table = getTable(tableName)
//        val schema = table.getSchema
////    val insert = table.newInsert
//        val insert = table.newUpsert()
//        val row = insert.getRow
//
//        cols.foreach{ case (column, newValue) => {
//            try {
//                KuduWriter.addColumnData(row, schema.getColumn(column.toLowerCase), newValue)
//            } catch {
//                case e: IllegalArgumentException => {
//                    logger.warn(s"Will not add ($column, $newValue) for ${table.getName}", e)
//                }
//            }
////      logger.info(s"Writing $column , $newValue")
//        }}
//        session.apply(insert)
//    }

}
