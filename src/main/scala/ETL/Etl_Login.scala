package ETL

import Utils.{DateHelper, SparkHelper}
import org.apache.spark.rdd.RDD
import java.lang.{Exception, System}

import Beans.Login
import CONSTANTS.CE04Constant
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession


object Etl_Login {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("ETL_CE04_LOGIN", CE04Constant.MASTER)

    val sc = spark.sparkContext

    val time = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val timeStr = DateHelper.formatTime(time, "yyyy-MM-dd")

    try {
      val rdd = sc.textFile(CE04Constant.LOG_ADDRESS + s"*.login.${timeStr}.log")

      //过滤
      val filterRdd = getFilterRdd(rdd)

      val ipRdd = sc.textFile(CE04Constant.IP_ADDRESS)
      val ipMapRdd: RDD[(Long, Long, String)] = ipRdd.map(line => {
        val arr = line.split("\\|")

        (arr(1).toLong, arr(2).toLong, arr(4))
      })
      val ipRule: Array[(Long, Long, String)] = ipMapRdd.collect()

      val ipRules = sc.broadcast(ipRule)


      //解析
      val mapRdd = getMapRDD(filterRdd, ipRules)


      //建表
      load2Table(spark, mapRdd)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }

  }

  /**
    * 过滤不符合规则的数据
    *
    * @param rdd
    * @return
    */
  def getFilterRdd(rdd: RDD[String]): RDD[String] = {
    //过滤
    val filterRdd = rdd.filter(line => {
      var flag = true
      try {
        val arr = line.split("\\|")

        if (arr.length != 13) {
          flag = false
        }

        val timeArr = arr(0).split(" ")
        val timestamp = timeArr(timeArr.length - 1)
        //时间戳
        if (timestamp == null || timestamp.length != 13) {
          flag = false
        }

        //有可能是空的 或者是"null" 分割之后可能会数据越界
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
        //ip地址

        if (arr(2).split("\\.").length != 4) flag = false

        //用户名
        if (arr(3) == null) {
          flag = false
        }

        //网站id
        if (arr(4) == null || arr(4) == "null") flag = false
        //网站类型 渠道
        if (arr(5) == null) flag = false
        //网站主域名
        if (arr(6) == null || arr(6) == "null") flag = false
        //网站制作期域名
        if (arr(7) == null) flag = false
        //登录方式
        if (arr(8) == null) flag = false
        //页面数量
        val pageNum = arr(9).split("=")(1)
        if (pageNum == null) flag = false
        //备份数量
        val backUp = arr(10).split("=")(1)
        if (backUp == null) {
          flag = false
        }
        //主题域名
        arr(11)
        //方法用时
        val time = arr(12).substring(0, arr(12).length - 2)
        if (time == null) flag = false
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
    * rdd装换成RDD[Login]
    *
    * @param filterRdd
    * @param ipRules
    * @return
    */
  def getMapRDD(filterRdd: RDD[String], ipRules: Broadcast[Array[(Long, Long, String)]]): RDD[Login] = {
    val mapRdd = filterRdd.mapPartitions(_.map(line => {
      val arr = line.split("\\|")

      val timeArr = arr(0).split(" ")
      val timestamp = timeArr(timeArr.length - 1)

      val process_timestamp = System.currentTimeMillis().toString

      val r1 = "[a-zA-Z]+\\/(\\d+\\.)+(\\d+)".r

      val str = r1.findAllIn(arr(1)).mkString(",")

      val browser_type = str.split(",")(2).split("/")(0)
      val browser_version = str.split(",")(2).split("/")(1)

      //ip地址
      val ip = arr(2)
      //获取city
      val ipArray = ipRules.value


      val index = search(ip2Long(arr(2)), ipArray)

      var city = ""
      if (index == -1) {
        city = "未知"
      } else {
        city = ipArray(index)._3
      }

      val user_name = arr(3)
      val site_id = arr(4)
      val channel = arr(5)

      val site_domain = arr(6)
      val site_create_name = arr(7)

      val login_method = arr(8)
      val page_num = arr(9).split("=")(1).toInt
      val backup_num = arr(10).split("=")(1).toInt

      val themeArr: Array[String] = arr(11).split("=")

      var theme = ""
      if (themeArr.length == 1) {

      } else {
        theme = themeArr(1)
      }

      var load_time = 0L
      if (arr(12) != null && arr(12).length > 3) {
        load_time = arr(12).substring(0, arr(12).length - 2).toLong
      } else {
        load_time = 0
      }

      Login(timestamp, process_timestamp, browser_type, browser_version, ip, city, user_name, site_id,
        channel, site_domain, site_create_name, login_method, page_num, backup_num, theme, load_time)
      //      val day = DateHelper.formatTime(timestamp.toLong, "yyyyMMdd")
    }))

    mapRdd
  }

  //toDF() 写入表中
  def load2Table(spark: SparkSession, mapRdd: RDD[Login]): Unit = {
    import spark.implicits._
    mapRdd.toDF().createOrReplaceTempView("base_login")
    val day = DateHelper.formatTime(System.currentTimeMillis().toLong - 24 * 60 * 60 * 1000, "yyyyMMdd")

    //导入数据
    spark.sql(
      s"""
         |insert overwrite table ods.ODS_CE04_LOG_LOGIN partition(day="${day}")
         |select
         |timestamp,
         |process_timestamp,
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
         |load_time
         |from base_login
      """.stripMargin)
  }

  //二分查找ip在ipAll.txt中的位置
  def search(ip: Long, ipArray: Array[(Long, Long, String)]): Int = {
    var start = 0
    var end = ipArray.length - 1

    while (start <= end) {
      var middle = (start + end) / 2

      var minddleKey = ipArray(middle)
      val middleKeyStart = minddleKey._1
      val middleKeyEnd = minddleKey._2

      if (ip >= middleKeyStart && ip <= middleKeyEnd) {
        return middle
      } else if (ip > middleKeyEnd) {
        start = middle + 1
      } else {
        end = middle - 1
      }
    }
    -1
  }

  //ip地址转换成16进制
  def ip2Long(ip: String): Long = {
    val ipArr = ip.split("\\.")
    val l1 = ipArr(0).toLong * Math.pow(2, 24)
    val l2 = ipArr(1).toLong * Math.pow(2, 16)
    val l3 = ipArr(2).toLong * Math.pow(2, 8)
    val l4 = ipArr(3).toLong

    (l1 + l2 + l3 + l4).toLong
  }
}
