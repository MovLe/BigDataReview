package Demo09SaveToMySQL

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @ClassName SaveToMySQL
 * @MethodDesc: 用SparkSession把结果写到MySQL
 * @Author Movle
 * @Date 11/11/20 9:00 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object SaveToMySQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("save to mysql").getOrCreate()

    val personRDD = spark.sparkContext.textFile("/users/macbook/testinfo/student.txt").map(_.split("\t"))

    val schema = StructType(
      List(
        StructField("id",IntegerType),
        StructField("name",StringType),
        StructField("age",IntegerType)))

    val rowRDD=personRDD.map(p=>Row(p(0).toInt,p(1),p(2).toInt))

    val personDF = spark.createDataFrame(rowRDD,schema)

    personDF.createOrReplaceTempView("t_student")

    val result = spark.sql("select * from t_student order by age desc")

    val props = new Properties()

    props.setProperty("user","root")
    props.setProperty("password","000000")

    result.write.mode("append").jdbc("jdbc:mysql://192.168.31.121:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "student", props)

    spark.stop()
  }

}
