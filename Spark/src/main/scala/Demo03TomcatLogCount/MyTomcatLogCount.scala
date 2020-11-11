package Demo03TomcatLogCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName MyTomcatLogCount
 * @MethodDesc: 统计Tomcat日志访问最多的两个页面
 * @Author Movle
 * @Date 11/11/20 4:44 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object MyTomcatLogCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Tomcat Log Count")

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

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.sortBy(_._2,false)

    rdd3.take(2).foreach(println)

    sc.stop()

  }

}
