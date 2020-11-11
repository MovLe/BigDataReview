package Demo01WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName WordCount
 * @MethodDesc: 用Scala写一个WordCount程序，本地
 * @Author Movle
 * @Date 11/11/20 4:11 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala WordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val result = sc.textFile("hdfs://192.168.31.121:9000/data/a.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)

    result.foreach(println)

    sc.stop()
  }

}
