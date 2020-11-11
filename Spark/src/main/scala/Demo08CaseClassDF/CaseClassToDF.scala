package Demo08CaseClassDF

import org.apache.spark.sql.SparkSession

/**
 * @ClassName CaseClassToDF
 * @MethodDesc: 使用case class创建DF
 * @Author Movle
 * @Date 11/11/20 8:50 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object CaseClassToDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("case class demo").getOrCreate()

    val lineRDD = spark.sparkContext.textFile("/users/macbook/TestInfo/student.txt").map(_.split("\t"))

    val studentRDD = lineRDD.map(x => Student(x(0).toInt,x(1),x(2).toInt))

    import spark.sqlContext.implicits._

    val studentDF = studentRDD.toDF()

    studentDF.createOrReplaceTempView("it_student")

    spark.sql("select * from it_student").show

    spark.stop()

  }

}
