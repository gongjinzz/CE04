package SQL

import CONSTANTS.CE04Constant
import Utils.SparkHelper
import org.apache.spark.sql.SparkSession

object CreateDIMTables {

  def main(args: Array[String]): Unit = {
    val spark = SparkHelper.getSpark("CreateDIMTables", CE04Constant.MASTER)

    dim_ce04_channel_code(spark)
  }

  def dim_ce04_channel_code(spark: SparkSession): Unit = {
    spark.sql(
      """
        |create database if not exists dim
      """.stripMargin)

    spark.sql(
      """
        |CREATE external TABLE if not exists dim.dim_ce04_channel_code(
        |ID string    COMMENT '渠道id' ,
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
        |CHANNEL string    COMMENT '渠道编号' ,
        |PRODUCT string    ,
        |PRODUCTID string    ,
        |MANAGER string   ,
        |APITYPE string
        |)stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dim.db/dim_ce04_channel_code'
      """.stripMargin)
  }
}
