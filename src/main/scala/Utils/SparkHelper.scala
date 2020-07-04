package Utils

import java.util.Properties

import CONSTANTS.CE04Constant
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHelper {

  //获取spark
  def getSpark(name: String, master: String) = {

    val conf = new SparkConf().setAppName(name).setMaster(master)
      //      .set("spark.sql.shuffle.partitions", "20")
      //使用hive的serde
      .set("spark.sql.hive.convertMetastoreParquet", "false")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark
  }

  //获取没有hive的spark
  def getSparkNoHive(name: String, master: String) = {
    val conf = new SparkConf().setAppName(name).setMaster(master)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    spark
  }

  //hive读取写入mysql
  def hive2Mysql(spark: SparkSession, table: String, sql: String): Unit = {

    val url = CE04Constant.DM_URL
    val user = CE04Constant.DM_USER
    val password = CE04Constant.DM_PASSWORD
    val prop = new Properties()

    prop.put("user", user)
    prop.put("password", password)
    prop.put("driver", "com.mysql.jdbc.Driver")


    spark.sql(sql).write
      .mode(SaveMode.Append)
      //.option("batchsize", 10000)
      .jdbc(url, table, prop)

  }

}
