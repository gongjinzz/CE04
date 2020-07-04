package DM

import CONSTANTS.CE04Constant
import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.SparkSession

object DMCE042MysqlMonth {

  def main(args: Array[String]): Unit = {


    val spark = SparkHelper.getSpark("DMCE042Mysql", CE04Constant.MASTER)

    val month = DateHelper.getLastMonth()

    dm_ce04_r02_msitecnt(spark, month)

    dm_ce04_r07_mactrate(spark, month)

    dm_ce04_r05_merrrate(spark, month)
  }

  def dm_ce04_r02_msitecnt(spark: SparkSession, month: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |site_count_mth
         |from dm.dm_ce04_r02_msitecnt
         |where month='${month}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r02_msitecnt", sql)
  }

  def dm_ce04_r07_mactrate(spark: SparkSession, month: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |activity_count_mth
         |from dm.dm_ce04_r07_mactrate
         |where month='${month}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r07_mactrate", sql)
  }

  def dm_ce04_r05_merrrate(spark: SparkSession, month: String): Unit = {
    val sql =
      s"""
         |select
         |channel,
         |channel_name,
         |month,
         |fault_rate_mth
         |from dm.dm_ce04_r05_merrrate
         |where month='${month}'
      """.stripMargin

    SparkHelper.hive2Mysql(spark, "dm_ce04_r05_merrrate", sql)
  }
}
