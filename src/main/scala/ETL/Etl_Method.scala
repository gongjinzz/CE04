package ETL

import java.lang.Exception

import Beans.Method
import CONSTANTS.CE04Constant
import Utils.{DateHelper, SparkHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Etl_Method {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("Etl_CE04_Method", CE04Constant.MASTER)
    val sc = spark.sparkContext

    val time = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val timeStr = DateHelper.formatTime(time, "yyyy-MM-dd")

    try {
      val rdd = sc.textFile(CE04Constant.LOG_ADDRESS + s"*.newAction.${timeStr}.log")

      val filterRdd = getFilterRdd(rdd)

      val mapRdd = getMapRdd(filterRdd)

      load2Table(spark, mapRdd)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

  }

  //过滤不符合规则的数据
  def getFilterRdd(rdd: RDD[String]): RDD[String] = {
    val filterRdd = rdd.filter(line => {
      var flag = true
      try {
        val arr = line.split("\\|")

        if (arr.length != 11) flag = false
        /*
            1590735628289|Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36
      172.22.142.4|172.22.142.4|ZresMaker|1600030784|100001|www.yiqiang.euzhongtec.com|2001155011.test-pool1-site.make.yun300.cn|
      Filter_/arch/getArchitectuePanel||111ms
      0时间戳|1User Agent|2ip|3用户名|4网站id|5网站类型|6网站主域名|7网站制作期域名|8方法名称|9方法参数|10方法用时
             */

        val timeArr = arr(0).split(" ")
        val timestamp = timeArr(timeArr.length - 1)

        if (timestamp == null || timestamp.length != 13) flag = false

        if (arr(1) == null || arr(1).equals("null")) {
          flag = false
        } else {
          //浏览器版本以及类型
          val r1 = "[a-zA-Z]+\\/(\\d+\\.)+(\\d+)".r

          val str = r1.findAllIn(arr(1)).mkString(",")

          val strings = str.split(",")
          if (strings.length < 3) {
            flag = false
          } else {
            val ss = str.split(",")(2).split("/")
            if (ss.length < 2) {
              flag = false
            } else {
              val browser_type = str.split(",")(2).split("/")(0)
              val browser_version = str.split(",")(2).split("/")(1)
              if (browser_type == null || browser_version == null) {
                flag = false
              }
            }
          }
        }
        //ip
        if (arr(2) == null) flag = false
        /*//用户名
            if (arr(3) == null) flag = false
            //网站id
            if (arr(4) == null) flag = false
            //网站类型
            if (arr(5) == null) flag = false
            //网站域名
            if (arr(6) == null) flag = false
            //网站制作器域名
            if (arr(7) == null) flag = false*/
        //方法名称
        if (arr(8) == null || arr(8).length < 3) flag = false

        if (arr(3) == null || arr(4) == null || arr(5) == null || arr(6) == null || arr(7) == null) {
          flag = false
        }
      } catch {
        case ex: Exception => {
          flag = false
        }
      }
      //方法参数
      // 方法用时
      flag
    })
    filterRdd
  }

  //rdd转换成RDD[Method]
  def getMapRdd(filterRdd: RDD[String]): RDD[Method] = {
    val mapRDD = filterRdd.mapPartitions(_.map(line => {
      val arr = line.split("\\|")

      //0时间戳|1User Agent|2ip|3用户名|4网站id|5网站类型|6网站主域名|7网站制作期域名|8方法名称|9方法参数|10方法用时
      val timeArr = arr(0).split(" ")
      val timestamp = timeArr(timeArr.length - 1)

      val process_timestamp = System.currentTimeMillis().toString

      //浏览器版本以及类型
      val r1 = "[a-zA-Z]+\\/(\\d+\\.)+(\\d+)".r

      val str = r1.findAllIn(arr(1)).mkString(",")
      val browser_type = str.split(",")(2).split("/")(0)
      val browser_version = str.split(",")(2).split("/")(1)

      val ip = arr(2)

      val user_name = arr(3)
      val site_id = arr(4)

      val channel = arr(5)
      val site_domain = arr(6)
      val site_create_name = arr(7)

      val method_name = arr(8)
      val method_params = arr(9)
      val method_time = arr(10).substring(0, arr(10).length - 2).toLong

      Method(timestamp, process_timestamp, browser_type, browser_version, ip, user_name, site_id, channel,
        site_domain, site_create_name, method_name, method_params, method_time)
    }))
    mapRDD
  }

  //load进ODS层
  def load2Table(spark: SparkSession, mapRdd: RDD[Method]): Unit = {
    import spark.implicits._
    mapRdd.toDF().createOrReplaceTempView("base_method")

    //spark.sql("select * from base_method").show()

    val day = DateHelper.formatTime(System.currentTimeMillis() - 24 * 60 * 60 * 1000, "yyyyMMdd")
    spark.sql(
      s"""
         |insert overwrite table ods.ods_ce04_log_method partition (day=${day})
         |select
         |timestamp,
         |process_timestamp,
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
         |from base_method
           """.stripMargin)

  }
}
