package Test

import Beans.OperateInfo
import Utils.SparkHelper
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

import scala.io.Source


object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("test", "local[2]")

    test(spark)
  }

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
      """
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
      """.stripMargin).show()

  }


  def test(spark: SparkSession): Unit ={

    spark.sql(
      """
        |create database dim
      """.stripMargin)
  }


}
