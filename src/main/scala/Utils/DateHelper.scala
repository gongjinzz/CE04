package Utils

import java.lang.System
import java.text.SimpleDateFormat
import java.util.Date

object DateHelper {

  def formatTime(timestamp: Long, formatType: String): String = {

    // val timestamp = System.currentTimeMillis()
    val date = new Date(timestamp)
    val format = new SimpleDateFormat(formatType)

    val str: String = format.format(date)
    str
  }

  //获取昨天的日期 yyyyMMdd
  def getYesterday() = {
    val timestamp = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val date = new Date(timestamp)
    val formatter = new SimpleDateFormat("yyyyMMdd")
    val str = formatter.format(date)

    str
  }

  //每月1号执行  获取上个月的日期 yyyyMM
  def getLastMonth(): String = {
    val timestamp = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val date = new Date(timestamp)

    val formatter = new SimpleDateFormat("yyyyMM")
    val str = formatter.format(date)

    str
  }

  //获取本月的日期
  def getCurrentMonth(): String = {
    val timestamp = System.currentTimeMillis()
    val date = new Date(timestamp)
    val formatter = new SimpleDateFormat("yyyyMM")
    val month = formatter.format(date)

    month
  }

  //test
  def main(args: Array[String]): Unit = {
    //    println(formatTime(System.currentTimeMillis(), "yyyyMMdd"))
    //    println(getYesterday())

    println(getCurrentMonth())
  }

}
