package ETL

import java.io.FileNotFoundException

import Utils.{DateHelper, SparkHelper}
import Beans.Error
import CONSTANTS.CE04Constant
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * create by hezongjin
  * time:2020-06-20
  */
object Etl_Error {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("Etl_CE04_Error", CE04Constant.MASTER)

    val sc = spark.sparkContext

    val time = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val timeStr = DateHelper.formatTime(time, "yyyy-MM-dd")
    try {
      val rdd = sc.textFile(CE04Constant.LOG_ADDRESS + s"*.overtime.${timeStr}.log")
      //过滤不符合规则的信息
      val filterRdd = getFilterRDD(rdd)

      //RDD[Sting]->RDD[Error]
      val mapRdd = getMapRDD(filterRdd)

      //toDF 写入 表中
      load2Table(spark, mapRdd)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

  }

  /**
    * 过滤
    *
    * @param rdd 原始rdd
    * @return 过滤之后的rdd
    */
  def getFilterRDD(rdd: RDD[String]): RDD[String] = {
    val filterRdd = rdd.filter(line => {
      var flag = true

      try {
        val arr = line.split("\\|")

        if (arr.length != 10) flag = false
        /*
1590740422556|Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36
172.22.142.1|shen|1600032360|100001|null|2004225029.test-pool1-site.make.yun300.cn|blank1|{"navigationStart":1590740072948,"unloadEventStart":1590740073416,"unloadEventEnd":1590740073418,"redirectStart":0,"redirectEnd":0,"fetchStart":1590740072949,"domainLookupStart":1590740072949,"domainLookupEnd":1590740072949,"connectStart":1590740072949,"connectEnd":1590740072949,"secureConnectionStart":0,"requestStart":1590740072951,"responseStart":1590740073412,"responseEnd":1590740073413,"domLoading":1590740073422,"domInteractive":1590740088311,"domContentLoadedEventStart":1590740088311,"domContentLoadedEventEnd":1590740088324,"domComplete":1590740092518,"loadEventStart":1590740092518,"loadEventEnd":1590740092519}

0时间戳|Use1r Agent|2ip|3用户名|4网站id|5网站类型|6网站主域名|7 网站制作期域名|8页面链接|9页面信息
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

        //用户名
        if (arr(3) == null) flag = false
        //网站id
        if (arr(4) == null) flag = false
        //网站类型
        if (arr(5) == null) flag = false
        //网站主域名
        if (arr(6) == null || arr(6) == "null") flag = false
        // 网站制作期域名
        if (arr(7) == null) flag = false
        //页面链接
        if (arr(8) == null) flag = false
        //页面信息
        if (arr(9) == null) flag = false
      } catch {
        case ex: Exception => {
          flag = false
        }
      }
      flag
    })
    filterRdd
  }

  /**
    * rdd转换成RDD[Errot]
    *
    * @param filterRdd
    * @return
    */
  def getMapRDD(filterRdd: RDD[String]): RDD[Error] = {
    val mapRdd = filterRdd.mapPartitions(_.map(line => {

      val arr = line.split("\\|")


      val timeArr = arr(0).split(" ")
      val timestamp = timeArr(timeArr.length - 1)
      val process_timestamp = System.currentTimeMillis().toString
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
      val page_link = arr(8)
      val error_info = arr(9)

      Error(timestamp, process_timestamp, browser_type, browser_version, ip,
        user_name, site_id, channel, site_domain, site_create_name, page_link, error_info)
    }))
    mapRdd
  }

  /**
    * ETL之后的数据load进ods层的error
    *
    * @param spark
    * @param mapRdd
    */
  def load2Table(spark: SparkSession, mapRdd: RDD[Error]): Unit = {
    import spark.implicits._
    mapRdd.toDF().createOrReplaceTempView("base_error")

    val day = DateHelper.formatTime(System.currentTimeMillis() - 24 * 60 * 60 * 1000, "yyyyMMdd")

    spark.sql(
      s"""
         |insert overwrite table ods.ods_ce04_log_error partition (day=${day})
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
         |page_link,
         |error_info
         |from base_error
          """.stripMargin)
  }

}
