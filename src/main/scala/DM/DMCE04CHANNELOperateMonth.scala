package DM

import Beans.OperateInfo
import Utils.{DateHelper, SparkHelper}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession
import java.lang.System
import java.text.SimpleDateFormat
import java.util.Date

import CONSTANTS.CE04Constant

import scala.io.Source

object DMCE04CHANNELOperateMonth {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("DMCE04CHANNELOperateMonth", CE04Constant.MASTER)

    //    val month = DateHelper.getCurrentMonth()

    val month = DateHelper.getLastMonth()

    dm_ce04_r01_dopermon(spark, month)
  }

  //每月运营资源统计
  def dm_ce04_r01_dopermon(spark: SparkSession, month: String): Unit = {

    val sourceStr = Source.fromURL("http://rpdesign.yun300.cn/api/resource/getOperDataApi?type=ceself&num=100", "utf-8").mkString

    val jsonObect: JSONObject = JSON.parseObject(sourceStr)

    //组件数量
    val unitCountNum = jsonObect.getString("unitCount").toLong
    //内容组数量
    val contentGroupCountNum = jsonObect.getString("contentGroupCount").toLong
    //页面数量
    val contentCountNum = jsonObect.getString("contentCount").toLong
    //应用组数量
    val appGroupCountNum = jsonObect.getString("appGroupCount").toLong
    //主题数量
    val sitetmplCountNum = jsonObect.getString("sitetmplCount").toLong


    val operateInfo = OperateInfo(sourceStr, unitCountNum, contentGroupCountNum, contentCountNum, appGroupCountNum, sitetmplCountNum)

    val list = List(operateInfo)

    val rdd = spark.sparkContext.parallelize(list)

    import spark.implicits._
    rdd.toDF().createOrReplaceTempView("base_operateInfoDF")

    spark.sql(
      s"""
         |insert overwrite table dm.dm_ce04_r01_dopermon partition (month=${month})
         |select
         |sourceStr,
         |unitCountNum,
         |contentGroupCountNum,
         |contentCountNum,
         |appGroupCountNum,
         |sitetmplCountNum,
         |unitCountNum/(unitCountNum+contentGroupCountNum+contentCountNum+appGroupCountNum+sitetmplCountNum) as unitCountRate,
         |contentGroupCountNum/(unitCountNum+contentGroupCountNum+contentCountNum+appGroupCountNum+sitetmplCountNum)as contentGroupCountRate,
         |contentCountNum/(unitCountNum+contentGroupCountNum+contentCountNum+appGroupCountNum+sitetmplCountNum) as contentCountRate,
         |appGroupCountNum/(unitCountNum+contentGroupCountNum+contentCountNum+appGroupCountNum+sitetmplCountNum) as appGroupCountRate,
         |sitetmplCountNum/(unitCountNum+contentGroupCountNum+contentCountNum+appGroupCountNum+sitetmplCountNum) as sitetmplCountRate
         |from base_operateInfoDF
      """.stripMargin)
  }
}
