package DW

import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.SparkSession
import java.lang.System

import CONSTANTS.CE04Constant

object DWCE04Channel {

  def main(args: Array[String]): Unit = {
    val spark = SparkHelper.getSpark("DWCE04Channel", CE04Constant.MASTER)


    val day = DateHelper.formatTime(System.currentTimeMillis() - 24 * 60 * 60 * 1000, "yyyyMMdd")

    dw_ce04_channel_login(spark, day)

    dw_ce04_log_method(spark, day)

    dw_ce04_channel_error(spark, day)

    dw_ce04_channel_newsite_day(spark, day)

    dw_ce04_channel_site_list_day(spark, day)

    dw_ce04_channel_site_count_day(spark, day)

    dw_ce04_channel_active_rate_day(spark, day)

    dw_ce04_channel_error_rate_day(spark, day)

    dw_ce04_channel_delete_history(spark, day)
  }


  //登录
  def dw_ce04_channel_login(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table dw.dw_ce04_channel_login partition (day=${day})
         |select
         |timestamp,
         |browser_type,
         |browser_version,
         |ip,
         |city,
         |user_name,
         |site_id,
         |channel,
         |site_domain,
         |site_create_name,
         |login_method,
         |page_num,
         |backup_num,
         |theme,
         |load_time,
         |case
         |when load_time = 0 then 0
         |when load_time < 10000 then 1
         |else 2 end as load_time_mark
         |from ods.ods_ce04_log_login
         |where day=${day}
      """.stripMargin)


  }

  //方法
  def dw_ce04_log_method(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table dw.dw_ce04_log_method partition (day=${day})
         |select
         |timestamp,
         |browser_type,
         |browser_version,
         |ip,
         |user_name,
         |site_id,
         |channel,
         |site_domain,
         |site_create_name,
         |method_name,
         |method_params,
         |method_time
         |from ods.ods_ce04_log_method
         |where day=${day}
      """.stripMargin)
  }

  //异常
  def dw_ce04_channel_error(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table dw.dw_ce04_channel_error partition (day=${day})
         |select
         |timestamp,
         |browser_type,
         |browser_version,
         |ip,
         |user_name,
         |site_id,
         |channel,
         |site_domain,
         |site_create_name,
         |page_link,
         |error_info
         |from ods.ods_ce04_log_error
         |where day=${day}
      """.stripMargin).show()
  }

  //删除记录
  def dw_ce04_channel_delete_history(spark: SparkSession, day: String) {

    spark.sql(
      s"""
         |insert overwrite table dw.dw_ce04_channel_delete_history partition (day=${day})
         |select
         |`timestamp`,
         |browser_type,
         |browser_version,
         |ip,
         |user_name,
         |site_id,
         |channel,
         |site_domain,
         |site_create_name,
         |split(split(method_params,',')[1],'=')[1] as page_name,
         |method_time,
         |from_unixtime(cast (substr(`timestamp`,0,10) as bigint),'MM-dd-HH:mm:ss') as delete_time
         |from dw.dw_ce04_log_method
         |where method_name = '/page/deletePage' and day =${day}
      """.stripMargin).show()
  }

  //今日新注册的网站 使用site_domain 网站域名进行计算
  def dw_ce04_channel_newsite_day(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
        |with t1 as(
        |select
        |site_domain
        |from ods.ods_ce04_log_login
        |where day='${day}'
        |group by site_domain
        |),
        |tmp as(
        |select
        |site_domain from dw.dw_ce04_channel_newsite_day
        |where day < '${day}'
        |),
        |t2 as(
        |select
        |t1.site_domain as site_domain
        |from t1 left join  tmp
        |on t1.site_domain=tmp.site_domain
        |where tmp.site_domain is null
        |),
        |t3 as(
        |select
        |t2.site_domain as site_domain,
        |odstmp.timestamp as timestamp,
        |odstmp.ip as ip,
        |odstmp.city as city,
        |odstmp.channel as channel,
        |odstmp.user_name as user_name
        |from t2 left join ods.ods_ce04_log_login odstmp
        |on t2.site_domain=odstmp.site_domain
        |),
        |t4 as(
        |select
        |t3.user_name as user_name,
        |t3.timestamp as timestamp,
        |t3.ip as ip,
        |t3.city as city,
        |t3.channel as channel,
        |t3.site_domain as site_domain,
        |row_number() over(distribute by site_domain sort by `timestamp`) as rk
        |from t3
        |)
        |insert overwrite table dw.dw_ce04_channel_newsite_day partition (day=${day})
        |select
        |user_name,
        |ip,
        |city,
        |channel,
        |site_domain
        |from t4
        |where rk=1
      """.stripMargin).show()

  }

  //网站清单
  def dw_ce04_channel_site_list_day(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |site_domain,
         |site_create_name,
         |load_time_mark,
         |browser_type,
         |browser_version,
         |page_num,
         |backup_num,
         |timestamp,
         |theme,
         |row_number() over(distribute by site_domain sort by timestamp desc)as rk
         |from dw.dw_ce04_channel_login
         |where day='${day}'
         |),
         |t2 as(
         |select
         |site_domain
         |from dw.dw_ce04_channel_error
         |where day='${day}'
         |group by site_domain
         |),
         |t3 as(
         |select
         |t1.channel as channel,
         |t1.site_domain as site_domain,
         |t1.site_create_name as site_create_name,
         |t1.load_time_mark as load_time_mark,
         |case
         |when t2.site_domain is not null then 1
         |else 0 end as load_error_mark,
         |t1.browser_type as browser_type,
         |t1.browser_version as browser_version,
         |t1.page_num as page_num,
         |t1.backup_num as backup_num,
         |from_unixtime(substr(t1.timestamp,1,10),'MM-dd-HH:mm') as last_login,
         |t1.theme as theme
         |from t1 left join t2
         |on t1.site_domain=t2.site_domain
         |where t1.rk=1
         |),
         |t4 as(
         |select
         |t3.channel as channel,
         |t3.site_domain as site_domain,
         |t3.site_create_name as site_create_name,
         |t3.load_time_mark as load_time_mark,
         |t3.load_error_mark as load_error_mark,
         |t3.browser_type as browser_type,
         |t3.browser_version as browser_version,
         |t3.page_num as page_num,
         |t3.backup_num as backup_num,
         |t3.last_login as last_login,
         |t3.theme as theme,
         |day as start_date
         |from t3 left join dw.dw_ce04_channel_newsite_day tmp
         |on t3.site_domain=tmp.site_domain
         |),
         |t5 as(
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
         |from dw.dw_ce04_channel_site_list_day
         |where day='${day}'
         |)
         |insert overwrite table dw.dw_ce04_channel_site_list_day partition (day=${day})
         |select
         |case when t4.channel is not null then t4.channel else t5.channel end  as channel,
         |case when t4.site_domain is not  null then t4.site_domain else t5.site_domain end  as site_domain,
         |case when t4.site_create_name is not null then t4.site_create_name else t5.site_create_name end  as site_create_name,
         |case when t4.load_time_mark is not null then t4.load_time_mark else t5.load_time_mark end  as load_time_mark,
         |case when t4.load_error_mark is not null then t4.load_error_mark else t5.load_error_mark end  as load_error_mark,
         |case when t4.browser_type is not null then t4.browser_type else t5.browser_type end  as browser_type,
         |case when t4.browser_version is not null then t4.browser_version else t5.browser_version end  as browser_version,
         |case when t4.page_num is not null then t4.page_num else t5.page_num end  as page_num,
         |case when t4.backup_num is not null then t4.backup_num else t5.backup_num end  as backup_num,
         |case when t4.last_login is not null then t4.last_login else t5.last_login end  as last_login,
         |case when t4.theme is not null then t4.theme else t5.theme end  as theme,
         |case when t4.start_date is not null then t4.start_date else t5.start_date end  as start_date
         |from
         |t5 full outer join t4
         |on t5.site_domain=t4.site_domain
        """.stripMargin).show()

  }

  //今天每个渠道 一共注册了多少网站
  def dw_ce04_channel_site_count_day(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table dw.dw_ce04_channel_site_count_day partition (day=${day})
         |select
         |channel,
         |substr(day,0,6) as month,
         |count(*) as site_count_day
         |from dw.dw_ce04_channel_newsite_day
         |where day='${day}'
         |group by channel,day
      """.stripMargin).show()
  }

  //今日活跃度
  def dw_ce04_channel_active_rate_day(spark: SparkSession, day: String): Unit = {

    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |site_domain,
         |day
         |from dw.dw_ce04_channel_login
         |where day='${day}'
         |group by channel,site_domain,day
         |)
         |insert overwrite table dw.dw_ce04_channel_active_rate_day partition (day=${day})
         |select
         |channel,
         |substr(day,0,6) as month,
         |count(*) as activity_count_day
         |from t1
         |group by channel,day
      """.stripMargin).show()

    //    insert overwrite table dw.dw_ce04_channel_active_rate_day partition (day=${day})
  }

  //今日错误率
  def dw_ce04_channel_error_rate_day(spark: SparkSession, day: String): Unit = {
    spark.sql(
      s"""
         |with t1 as(
         |select
         |channel,
         |site_domain,
         |day
         |from dw.dw_ce04_channel_error
         |where day=${day}
         |group by channel,site_domain,day
         |),
         |t2 as(
         |select
         |channel,
         |count(*) as error_cnt,
         |day
         |from t1
         |group by channel,day
         |),
         |t3 as(
         |select
         |channel,
         |site_domain,
         |day
         |from dw.dw_ce04_log_method
         |where day=${day}
         |group by channel,site_domain,day
         |),
         |t4 as(
         |select
         |channel,
         |count(*) as operate_cnt,
         |day
         |from t3
         |group by channel,day
         |)
         |insert overwrite table dw.dw_ce04_channel_error_rate_day partition (day=${day})
         |select
         |t4.channel as channel,
         |substr(t4.day,0,6) as month,
         |case when t2.error_cnt is not null then t2.error_cnt else 0 end as error_cnt,
         |case when t4.operate_cnt is not null then t4.operate_cnt  else 0 end as operate_cnt,
         |case when t2.error_cnt is null then 0.0
         |when t4.operate_cnt is null  then 0.0
         |else error_cnt/operate_cnt end as fault_rate_day
         |from
         |t4 left join t2
         |on t4.channel=t2.channel
      """.stripMargin).show()
  }
}
