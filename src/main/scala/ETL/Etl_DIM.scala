package ETL

import java.util.Properties

import CONSTANTS.CE04Constant
import Utils.SparkHelper

object Etl_DIM {
  def main(args: Array[String]): Unit = {

    //每天全量抽取一次渠道维表
    val spark = SparkHelper.getSpark("DIM", CE04Constant.MASTER)


    val url = CE04Constant.DIM_URL
    val prop = new Properties()
    prop.put("user", CE04Constant.DIM_USER)
    prop.put("password", CE04Constant.DIM_PASSWORD)
    prop.put("driver", "com.mysql.jdbc.Driver")

    val df = spark.read.jdbc(url, "PRODUCTLINE ", prop)

    df.createOrReplaceTempView("base_productline")

    spark.sql(
      """
        |insert overwrite table dim.dim_ce04_channel_code
        |select
        |ID,
        |NAME,
        |UNITTYPE,
        |PRODUCT,
        |PRODUCTID,
        |MANAGER,
        |APITYPE
        |from base_productline
      """.stripMargin)

  }
}
