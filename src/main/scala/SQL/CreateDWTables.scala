package SQL

import CONSTANTS.CE04Constant
import Utils.SparkHelper
import org.apache.spark.sql.SparkSession

object CreateDWTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("CreateDWTables", CE04Constant.MASTER)

    import spark.sql
    sql(
      """
        |create database if not exists dw
      """.stripMargin)

    createdw_ce04_channel_login(spark)

    createdw_ce04_log_method(spark)

    createdw_ce04_channel_error(spark)

    createdw_ce04_channel_delete_history(spark)

    createdw_ce04_channel_newsite_day(spark)

    createdw_ce04_channel_site_list_day(spark)

    createdw_ce04_channel_site_count_day(spark)

    createdw_ce04_channel_active_rate_day(spark)

    createdw_ce04_channel_error_rate_day(spark)
  }

  def createdw_ce04_channel_login(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external  TABLE if not exists dw.dw_ce04_channel_login(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器版本' ,
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
        |LOAD_TIME BIGINT    COMMENT '加载时长' ,
        |LOAD_TIME_MARK INT    COMMENT '加载标识'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_login'
      """.stripMargin)
  }

  def createdw_ce04_log_method(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_log_method(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器版本' ,
        |BROWSER_VERSION string    COMMENT '浏览器版本' ,
        |IP string    COMMENT 'ip地址' ,
        |USER_NAME string    COMMENT '用户名' ,
        |SITE_ID string    COMMENT '网站id' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站主域名' ,
        |SITE_CREATE_NAME string    COMMENT '网站制作期域名' ,
        |METHOD_NAME string    COMMENT '方法名称' ,
        |METHOD_PARAMS string    COMMENT '方法参数' ,
        |METHOD_TIME bigint    COMMENT '方法用时'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_log_method'
      """.stripMargin)
  }

  def createdw_ce04_channel_error(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_error(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器版本' ,
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
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_error'
      """.stripMargin)

  }

  def createdw_ce04_channel_delete_history(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_delete_history(
        |`TIMESTAMP` string    COMMENT '时间戳' ,
        |BROWSER_TYPE string    COMMENT '浏览器版本' ,
        |BROWSER_VERSION string    COMMENT '浏览器版本' ,
        |IP string    COMMENT 'ip地址' ,
        |USER_NAME string    COMMENT '用户名' ,
        |SITE_ID string    COMMENT '网站id' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站域名' ,
        |SITE_CREATE_NAME string    COMMENT '网站制作期域名' ,
        |PAGE_NAME string    COMMENT '页面名称' ,
        |METHOD_TIME BIGINT    COMMENT '方法用时' ,
        |DELETE_TIME string    COMMENT '删除时间'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_delete_history'
      """.stripMargin)
  }

  def createdw_ce04_channel_newsite_day(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_newsite_day(
        |USER_NAME string    COMMENT '用户名' ,
        |IP string    COMMENT 'IP地址' ,
        |CITY string    COMMENT '城市' ,
        |CHANNEL string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '渠道域名'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_newsite_day'
      """.stripMargin)
  }

  def createdw_ce04_channel_site_list_day(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_site_list_day(
        |CHANNEL string    COMMENT '渠道编码' ,
        |SITE_DOMAIN string    COMMENT '网站名称' ,
        |SITE_CREATE_NAME string    COMMENT '网站制作器域名' ,
        |LOAD_TIME_MARK INT    COMMENT '加载时长' ,
        |LOAD_ERROR_MARK INT    COMMENT '加载异常标识' ,
        |BROWSER_TYPE string    COMMENT '浏览器类型' ,
        |BROWSER_VERSION string    COMMENT '浏览器版本' ,
        |PAGE_NUM INT    COMMENT '页面数量' ,
        |BACKUP_NUM INT    COMMENT '备份数量' ,
        |LAST_LOGIN string    COMMENT '最后登录时间' ,
        |THEME string    COMMENT '主题' ,
        |START_DATE string    COMMENT '注册日期'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_site_list_day'
      """.stripMargin)
  }

  def createdw_ce04_channel_site_count_day(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_site_count_day(
        |CHANNEL string    COMMENT '渠道' ,
        |MONTH string    COMMENT '月份' ,
        |SITE_COUNT_DAY BIGINT    COMMENT '新增网站'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_site_count_day'
      """.stripMargin)
  }

  def createdw_ce04_channel_active_rate_day(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists  dw.dw_ce04_channel_active_rate_day(
        |CHANNEL string    COMMENT '渠道' ,
        |MONTH string    COMMENT '月份' ,
        |ACTIVITY_COUNT_DAY BIGINT    COMMENT '活跃度'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_active_rate_day'
      """.stripMargin)
  }

  def createdw_ce04_channel_error_rate_day(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dw.dw_ce04_channel_error_rate_day(
        |CHANNEL string    COMMENT '渠道' ,
        |MONTH string    COMMENT '月份' ,
        |ERROR_CNT BIGINT    COMMENT '故障数' ,
        |OPERATE_CNT BIGINT    COMMENT '操作数' ,
        |FAULT_RATE_DAY double    COMMENT '故障率'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dw.db/dw_ce04_channel_error_rate_day'
      """.stripMargin)
  }

}
