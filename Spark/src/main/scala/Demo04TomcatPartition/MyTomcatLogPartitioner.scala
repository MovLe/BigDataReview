package Demo04TomcatPartition

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ClassName MyTomcatLogPartitioner
 * @MethodDesc: 分区
 * @Author Movle
 * @Date 11/11/20 5:03 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object MyTomcatLogPartitioner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tomcat Partition").setMaster("local")
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

          (jspName,line)
        }
      )

    val rdd2 = rdd1.map(_._1).distinct().collect()

    val myPartitioner = new MyWebPartitioner(rdd2)

    val rdd3 = rdd1.partitionBy(myPartitioner)

    rdd3.saveAsTextFile("hdfs://192.168.31.121:9000/out/1113")



    rdd3.foreach(println)
    sc.stop()
  }
  class MyWebPartitioner(jspList:Array[String]) extends Partitioner{

    val partitionMap = new mutable.HashMap[String,Int]()

    var partId=0

    for(jsp <-jspList){
      partitionMap.put(jsp,partId)
      partId+=1
    }

    override def numPartitions: Int = partitionMap.size

    override def getPartition(key: Any): Int = partitionMap.getOrElse(key.toString,0)
  }


}
