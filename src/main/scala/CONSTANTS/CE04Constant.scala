package CONSTANTS

object CE04Constant {


  val HDFS = "hdfs://bdpcluster"

  //维表url 用户名 密码

  val DIM_URL = "jdbc:mysql://10.20.1.100:3306/NDESIGN?useUnicode=true&characterEncoding=utf8"
  //  val DIM_URL = "jdbc:mysql://10.12.40.206:3306/NDESIGN?useUnicode=true&characterEncoding=utf8"
  val DIM_USER = "readuserbi"
  val DIM_PASSWORD = "b45ce2224c"


  val MASTER = "yarn"

  val TEST_MASTER = "local[2]"

  //ip文本存放地址
  val IP_ADDRESS = "hdfs://bdpcluster/user/ce04/spark/ip/ipAll.txt"

  //log存放文件夹
  val LOG_ADDRESS = "hdfs://bdpcluster/user/ce04/spark/log/"

  //dm mysql地址 用户名密码
  val DM_URL = "jdbc:mysql://10.20.2.245:3306/rtm_server?useUnicode=true&characterEncoding=utf8"

  val DM_USER = "mycat"

  val DM_PASSWORD = "12345678"
}
