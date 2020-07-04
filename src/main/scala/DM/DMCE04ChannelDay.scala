package DM

import java.util.Properties

import CONSTANTS.CE04Constant
import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source


object DMCE04ChannelDay {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("DMCE04ChannelDay", CE04Constant.MASTER)


    val day = DateHelper.formatTime(System.currentTimeMillis() - 24 * 60 * 60 * 1000, "yyyyMMdd")

    dm_ce04_r08_duserdis(spark, day)

    dm_ce04_r11_dsiteerr(spark, day)

    dm_ce04_r10_ddelhis(spark, day)

    dm_ce04_r06_dactrate(spark, day)

    dm_ce04_r04_derrrate(spark, day)

    dm_ce04_r03_dsitecnt(spark, day)

    dm_ce04_r09_dsitelit(spark, day)

  }

  //今日注册的用户分布
  def dm_ce04_r08_duserdis(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |city,
         |count(*) as user_dis_count
         |from dw.dw_ce04_channel_newsite_day
         |where day='${day}'
         |group by channel,city
         |)
         |insert overwrite table dm.dm_ce04_r08_duserdis partition (day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.city as city,
         |t1.user_dis_count as user_dis_count
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin).show()


  }

  //网站异常
  def dm_ce04_r11_dsiteerr(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |site_domain,
         |page_link,
         |error_info,
         |from_unixtime(cast (substr(timestamp,0,10) as bigint),'MM-dd-HH:mm') as error_time
         |from dw.dw_ce04_channel_error
         |where day='${day}'
         |)
         |insert overwrite table dm.dm_ce04_r11_dsiteerr partition (day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.site_domain as site_domain,
         |t1.page_link as page_link,
         |t1.error_info as error_info,
         |t1.error_time as error_time
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin).show()
  }

  //删除记录
  def dm_ce04_r10_ddelhis(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |site_domain,
         |page_name,
         |delete_time
         |from dw.dw_ce04_channel_delete_history
         |where day='${day}'
         |)
         |insert overwrite table dm.dm_ce04_r10_ddelhis partition (day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.site_domain as site_domain,
         |t1.page_name as page_name,
         |t1.delete_time as delete_time
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin).show()

  }

  //活跃度
  def dm_ce04_r06_dactrate(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |month,
         |activity_count_day
         |from dw.dw_ce04_channel_active_rate_day
         |where day='${day}'
         |)
         |insert overwrite table dm.dm_ce04_r06_dactrate partition (day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.month as month,
         |t1.activity_count_day as activity_count_day
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin)
  }

  //异常率
  def dm_ce04_r04_derrrate(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |month,
         |error_cnt,
         |operate_cnt,
         |fault_rate_day
         |from dw.dw_ce04_channel_error_rate_day
         |where day='${day}'
         |)
         |insert overwrite table dm.dm_ce04_r04_derrrate partition (day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.month as month,
         |t1.error_cnt as error_cnt,
         |t1.operate_cnt as operate_cnt,
         |t1.fault_rate_day as fault_rate_day
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin)
  }

  //今日每个渠道一共有多少网站
  def dm_ce04_r03_dsitecnt(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |max(month) as month,
         |sum(site_count_day) as site_count_day
         |from dw.dw_ce04_channel_site_count_day
         |where day<=${day}
         |group by channel
         |)
         |insert overwrite table dm.dm_ce04_r03_dsitecnt partition(day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.month as month,
         |t1.site_count_day as site_count_day
         |from t1 left join dim.dim_ce04_channel_code as tmp
         |on t1.channel=tmp.channel
      """.stripMargin)
  }

  //网站清单
  def dm_ce04_r09_dsitelit(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
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
         |start_date
         |from  dw.dw_ce04_channel_site_list_day
         |where day='${day}'
         |)
         |insert overwrite table dm.dm_ce04_r09_dsitelit partition ( day=${day})
         |select
         |t1.channel as channel,
         |tmp.channel_name as channel_name,
         |t1.site_domain as site_domain,
         |t1.site_create_name as site_create_name,
         |t1.load_time_mark as load_time_mark,
         |t1.load_error_mark as load_error_mark,
         |t1.browser_type as browser_type,
         |t1.browser_version as browser_version,
         |t1.page_num as page_num,
         |t1.backup_num as backup_num,
         |t1.last_login as last_login,
         |t1.theme as theme,
         |t1.start_date as start_date
         |from t1 left join dim.dim_ce04_channel_code tmp
         |on t1.channel=tmp.channel
      """.stripMargin).show()
  }


}
