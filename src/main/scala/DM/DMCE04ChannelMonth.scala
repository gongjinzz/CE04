package DM

import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.SparkSession
import java.lang.System

import Beans.OperateInfo
import CONSTANTS.CE04Constant
import com.alibaba.fastjson.{JSON, JSONObject}

import scala.io.Source

object DMCE04ChannelMonth {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("DMCE04ChannelMonth", CE04Constant.MASTER)

    val month = DateHelper.formatTime(System.currentTimeMillis() - 24 * 60 * 60 * 1000, "yyyyMM")


    val day = DateHelper.getYesterday()

    dm_ce04_r02_msitecnt(spark, month, day)

    dm_ce04_r07_mactrate(spark, month)

    dm_ce04_r05_merrrate(spark, month)
  }

  //本月的数据 就是最后一天的数据
  def dm_ce04_r02_msitecnt(spark: SparkSession, month: String, day: String): Unit = {

    //时间戳 yyyyMM
    spark.sql(
      s"""
         |insert overwrite table dm.dm_ce04_r02_msitecnt partition (month=${month})
         |select
         |channel,
         |channel_name,
         |sum(site_count_day) as site_count_mth
         |from dm.dm_ce04_r03_dsitecnt
         |where day='${day}'
         |group by channel,channel_name
      """.stripMargin)

  }

  //本月每天活跃度取平均值
  def dm_ce04_r07_mactrate(spark: SparkSession, month: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table dm.dm_ce04_r07_mactrate partition (month=${month})
         |select
         |channel,
         |channel_name,
         |avg(activity_count_day) as activity_count_mth
         |from dm.dm_ce04_r06_dactrate
         |where month='${month}'
         |group by channel,channel_name
      """.stripMargin)

  }

  //本月每天的异常率取平均值
  def dm_ce04_r05_merrrate(spark: SparkSession, month: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table dm.dm_ce04_r05_merrrate partition (month=${month})
         |select
         |channel,
         |channel_name,
         |avg(fault_rate_day) as fault_rate_mth
         |from dm.dm_ce04_r04_derrrate
         |where month='${month}'
         |group by channel,channel_name
      """.stripMargin)

  }


}
