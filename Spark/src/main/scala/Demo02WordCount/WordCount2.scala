package Demo02WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName WordCount2
 * @MethodDesc: 用Scala写WordCount 集群
 * @Author Movle
 * @Date 11/11/20 4:24 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object WordCount2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Remote Scala WordCount")

    val sc = new SparkContext(conf)

    val result = sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))

    sc.stop()
  }

}
