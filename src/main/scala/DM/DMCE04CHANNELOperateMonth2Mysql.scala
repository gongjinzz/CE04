package DM

import CONSTANTS.CE04Constant
import Utils.{DateHelper, SparkHelper}
import org.apache.spark.sql.SparkSession

object DMCE04CHANNELOperateMonth2Mysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getSpark("DMCE04CHANNELOperateMonth2Mysql", CE04Constant.MASTER)

    val month = DateHelper.getLastMonth()

    dm_ce04_r01_dopermon(spark, month)
  }

  def dm_ce04_r01_dopermon(spark: SparkSession, month: String): Unit = {

    val sql =
      s"""
         |select
         |operation_info,
         |unitcountnum,
         |contentgroupcountnum,
         |contentcountnum,
         |appgroupcountnum,
         |sitetmplcountnum,
         |unitcountrate,
         |contentgroupcountrate,
         |contentcountrate,
         |appgroupcountrate,
         |sitetmplcountrate,
         |month
         |from dm.dm_ce04_r01_dopermon
         |where month='${month}'
      """.stripMargin


    SparkHelper.hive2Mysql(spark, "dm_ce04_r01_dopermon", sql)


  }
}


