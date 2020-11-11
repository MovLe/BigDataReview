package Demo05TomcatLogToMySQL

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName MyTomcatLogToMySQL
 * @MethodDesc: 将解析出的结果保存到mysql中
 * @Author Movle
 * @Date 11/11/20 6:02 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object MyTomcatLogToMySQL {

  def saveToMySQL(it:Iterator[(String, Int)] ) = {

    var conn:Connection = null
    var pst:PreparedStatement = null

    conn = DriverManager.getConnection("jdbc:mysql://192.168.31.121:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")

    pst = conn.prepareStatement("insert into mydata values (?,?)")

    it.foreach(data=>{
      pst.setString(1,data._1)
      pst.setInt(2,data._2)
      pst.executeUpdate()
    })

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Tomcat To MySQL")

    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("hdfs://192.168.31.121:9000/data/localhost_access_log.txt")
      .map(
        line =>{
          val index1 = line.indexOf("\"")
          val index2 = line.lastIndexOf("\"")
          val line1 = line.substring(index1+1,index2)

          val index3 = line1.indexOf(" ")
          val index4 = line1.lastIndexOf(" ")

          val line2 = line1.substring(index3,index4)

          val jspName = line2.substring(line2.lastIndexOf("/")+1)

          (jspName,1)
        }
      )
    rdd1.foreachPartition(saveToMySQL)

    sc.stop()
  }

}
