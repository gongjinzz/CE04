package SQL

import CONSTANTS.CE04Constant
import Utils.SparkHelper
import org.apache.spark.sql.SparkSession

object CreateODSTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("CreateODSTables", CE04Constant.MASTER)

    spark.sql(
      """
        |create database if not exists ods
      """.stripMargin)

    ods_ce04_log_login(spark)

    ods_ce04_log_method(spark)

    ods_ce04_log_error(spark)
  }

  def ods_ce04_log_login(sparkSession: SparkSession): Unit = {

    sparkSession.sql(
      """
        |CREATE external TABLE if not exists ods.ods_ce04_log_login(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |PROCESS_TIMESTAMP string    COMMENT '处理时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器类型' ,
        |BROWSER_VERSION string    COMMENT '浏览器版本' ,
        |IP string    COMMENT 'ip地址' ,
        |CITY string    COMMENT '城市' ,
        |USER_NAME string    COMMENT '用户名' ,
        |SITE_ID string    COMMENT '网站id' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站域名' ,
        |SITE_CREATE_NAME string    COMMENT '网站制作期域名' ,
        |LOGIN_METHOD string    COMMENT '登录方式' ,
        |PAGE_NUM INT    COMMENT '页面数量' ,
        |BACKUP_NUM INT    COMMENT '备份数量' ,
        |THEME string    COMMENT '主题域名' ,
        |LOAD_TIME BIGINT    COMMENT '加载时长'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/ods.db/ods_ce04_log_login'
      """.stripMargin)

  }

  def ods_ce04_log_method(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists ods.ods_ce04_log_method(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |PROCESS_TIMESTAMP string    COMMENT '处理时间戳' ,
        |BROWSER_TYPE string   COMMENT '浏览器版本' ,
        |BROWSER_VERSION string   COMMENT '浏览器版本' ,
        |IP string   COMMENT 'ip地址' ,
        |USER_NAME string    COMMENT '用户名' ,
        |SITE_ID string    COMMENT '网站id' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站主域名' ,
        |SITE_CREATE_NAME string    COMMENT '网站制作期域名' ,
        |METHOD_NAME string   COMMENT '方法名称' ,
        |METHOD_PARAMS string    COMMENT '方法参数' ,
        |METHOD_TIME bigint    COMMENT '方法用时'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/ods.db/ods_ce04_log_method'
      """.stripMargin)
  }

  def ods_ce04_log_error(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists  ods.ods_ce04_log_error(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |PROCESS_TIMESTAMP string    COMMENT '处理时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器类型' ,
        |BROWSER_VERSION string    COMMENT '浏览器版本' ,
        |IP string    COMMENT 'IP地址' ,
        |USER_NAME string    COMMENT '用户名' ,
        |SITE_ID string    COMMENT '网站id' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站域名' ,
        |SITE_CREATE_NAME string COMMENT '网站制作期域名',
        |PAGE_LINK string    COMMENT '页面链接' ,
        |ERROR_INFO string    COMMENT '错误信息'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/ods.db/ods_ce04_log_error'
      """.stripMargin)
  }
}
