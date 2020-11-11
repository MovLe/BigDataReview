package Demo07SparkSQLDataFrame

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @ClassName SparkSQLDataFrame
 * @MethodDesc: Spark SQL之使用SparkSession创建DataFrame执行sql
 * @Author Movle
 * @Date 11/11/20 8:01 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object SparkSQLDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataFrameSQL").getOrCreate()

    val personRDD=spark.sparkContext.textFile("/users/macbook/TestInfo/student.txt").map(_.split("\t"))

    val schema = StructType(
      List(
        StructField("id",IntegerType),
        StructField("name",StringType),
        StructField("age",IntegerType)
      )
    )

    val rowRDD = personRDD.map(p =>Row(p(0).toInt,p(1).trim,p(2).toInt))

    val personDF = spark.createDataFrame(rowRDD,schema)

    personDF.createOrReplaceTempView("t_person")

    val df = spark.sql("select * from t_person order by age desc")

    df.show()
    spark.stop()
  }

}
