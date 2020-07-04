package SQL

import CONSTANTS.CE04Constant
import Utils.SparkHelper
import org.apache.spark.sql.SparkSession

object CreateDMTables {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("CreateDMTables", CE04Constant.MASTER)

    spark.sql(
      """
        |create database if not exists dm
      """.stripMargin)

    dm_ce04_r08_duserdis(spark)

    dm_ce04_r11_dsiteerr(spark)

    dm_ce04_r10_ddelhis(spark)

    dm_ce04_r02_msitecnt(spark)

    dm_ce04_r06_dactrate(spark)

    dm_ce04_r04_derrrate(spark)

    dm_ce04_r07_mactrate(spark)

    dm_ce04_r03_dsitecnt(spark)

    dm_ce04_r05_merrrate(spark)

    dm_ce04_r09_dsitelit(spark)

    dm_ce04_r01_dopermon(spark)
  }

  def dm_ce04_r08_duserdis(spark: SparkSession): Unit = {
    spark.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r08_duserdis(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道' ,
        |CITY string    COMMENT '城市' ,
        |USER_DIS_COUNT BIGINT    COMMENT '人数'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r08_duserdis'
      """.stripMargin)
  }

  def dm_ce04_r11_dsiteerr(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r11_dsiteerr(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站名称' ,
        |PAGE_LINK string    COMMENT '页面链接' ,
        |ERROR_INFO string    COMMENT '错误信息' ,
        |ERROR_TIME string    COMMENT '异常时间'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r11_dsiteerr'
      """.stripMargin)
  }

  def dm_ce04_r10_ddelhis(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r10_ddelhis(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道' ,
        |SITE_DOMAIN string    COMMENT '网站域名' ,
        |PAGE_NAME string    COMMENT '页面名称' ,
        |DELETE_TIME string    COMMENT '删除时间'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r10_ddelhis'
      """.stripMargin)
  }

  def dm_ce04_r02_msitecnt(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r02_msitecnt(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
        |SITE_COUNT_MTH BIGINT    COMMENT '网站数量'
        |)
        |partitioned by (month string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r02_msitecnt'
      """.stripMargin)
  }

  def dm_ce04_r06_dactrate(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r06_dactrate(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string   COMMENT '渠道' ,
        |MONTH string   COMMENT '月份' ,
        |ACTIVITY_COUNT_DAY BIGINT    COMMENT '活跃度'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r06_dactrate'
      """.stripMargin)
  }

  def dm_ce04_r04_derrrate(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r04_derrrate(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
        |MONTH string    COMMENT '月份' ,
        |ERROR_CNT BIGINT    COMMENT '故障数' ,
        |OPERATE_CNT BIGINT    COMMENT '操作数' ,
        |FAULT_RATE_DAY double   COMMENT '故障率'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r04_derrrate'
      """.stripMargin)
  }

  def dm_ce04_r07_mactrate(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r07_mactrate(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道' ,
        |ACTIVITY_COUNT_MTH BIGINT    COMMENT '活跃数'
        |)
        |partitioned by (month string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r07_mactrate'
      """.stripMargin)
  }

  def dm_ce04_r03_dsitecnt(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r03_dsitecnt(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
        |MONTH string    COMMENT '月份' ,
        |SITE_COUNT_DAY BIGINT    COMMENT '新增网站'
        |)partitioned by (day string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r03_dsitecnt'
      """.stripMargin)
  }

  def dm_ce04_r05_merrrate(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r05_merrrate(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
        |FAULT_RATE_MTH double    COMMENT '故障率'
        |)
        |partitioned by (month string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r05_merrrate'
      """.stripMargin)
  }

  def dm_ce04_r09_dsitelit(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r09_dsitelit(
        |CHANNEL string COMMENT '渠道id',
        |CHANNEL_NAME string    COMMENT '渠道名字' ,
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
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r09_dsitelit'
      """.stripMargin)
  }

  def dm_ce04_r01_dopermon(sparkSession: SparkSession): Unit = {
    sparkSession.sql(
      """
        |CREATE external TABLE if not exists dm.dm_ce04_r01_dopermon(
        |OPERATION_INFO string    COMMENT '信息',
        |UNITCOUNTNUM  bigint COMMENT '组件数量',
        |CONTENTGROUPCOUNTNUM bigint COMMENT '内容组数量',
        |CONTENTCOUNTNUM bigint COMMENT '页面数量',
        |APPGROUPCOUNTNUM bigint COMMENT '应用组数量',
        |SITETMPLCOUNTNUM bigint COMMENT '主题数量',
        |UNITCOUNTRATE double COMMENT '组件占比',
        |CONTENTGROUPCOUNTRATE double COMMENT '内容组占比',
        |CONTENTCOUNTRATE double COMMENT '页面占比',
        |APPGROUPCOUNTRATE double COMMENT '应用组占比',
        |SITETMPLCOUNTRATE double COMMENT '主题占比'
        |)
        |partitioned by (month string)
        |stored as parquet
        |location 'hdfs://bdpcluster/warehouse/tablespace/external/hive/dm.db/dm_ce04_r01_dopermon'
      """.stripMargin)
  }
}
