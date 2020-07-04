package DM

import CONSTANTS.CE04Constant
import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.SparkSession

object DMCE042MysqlDay {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("DMCE042Mysql", CE04Constant.MASTER)
    val day = DateHelper.getYesterday()

    dm_ce04_r08_duserdis(spark, day)

    dm_ce04_r11_dsiteerr(spark, day)

    dm_ce04_r10_ddelhis(spark, day)

    dm_ce04_r06_dactrate(spark, day)

    dm_ce04_r04_derrrate(spark, day)

    dm_ce04_r03_dsitecnt(spark, day)


    dm_ce04_r09_dsitelit(spark, day)
  }


  def dm_ce04_r08_duserdis(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |city,
         |user_dis_count,
         |day
         |from dm.dm_ce04_r08_duserdis
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r08_duserdis", sql)
  }

  def dm_ce04_r11_dsiteerr(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |site_domain,
         |page_link,
         |error_info,
         |error_time,
         |day
         |from dm.dm_ce04_r11_dsiteerr
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r11_dsiteerr", sql)
  }

  def dm_ce04_r10_ddelhis(spark: SparkSession, day: String): Unit = {

    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |site_domain,
         |page_name,
         |delete_time,
         |day
         |from dm.dm_ce04_r10_ddelhis
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r10_ddelhis", sql)
  }


  def dm_ce04_r06_dactrate(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |day,
         |activity_count_day
         |from dm.dm_ce04_r06_dactrate
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r06_dactrate", sql)
  }

  def dm_ce04_r04_derrrate(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |day,
         |error_cnt,
         |operate_cnt,
         |fault_rate_day
         |from dm.dm_ce04_r04_derrrate
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r04_derrrate", sql)
  }

  def dm_ce04_r03_dsitecnt(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |day,
         |site_count_day
         |from dm.dm_ce04_r03_dsitecnt
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r03_dsitecnt", sql)
  }


  def dm_ce04_r09_dsitelit(spark: SparkSession, day: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |site_domain,
         |site_create_name,
         |load_time_mark,
         |load_error_mark,
         |browser_type,
         |browser_version,
         |page_num,
         |backup_num,
         |last_login,
         |theme,
         |start_date,
         |day
         |from dm.dm_ce04_r09_dsitelit
         |where day='${day}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r09_dsitelit", sql)
  }
}
