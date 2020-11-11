package Demo06JDBCRdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName MyJDBCRddDemo
 * @MethodDesc:
 * @Author Movle
 * @Date 11/11/20 6:46 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object MyJDBCRddDemo {

  val connection = () => {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://192.168.31.121:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")
  }

  def main(args: Array[String]): Unit = {

    //创建Spark对象
    val conf = new SparkConf().setAppName("My JDBC Rdd Demo").setMaster("local")
    val sc = new SparkContext(conf)

    val mysqlRDD = new JdbcRDD(sc,connection,"select * from emp where sal > ? and sal <= ?",900,2000, 2, r=>{
      //获取员工的姓名和薪水
      val ename = r.getString(2)
      val sal = r.getInt(4)
      (ename,sal)
    })

    val result = mysqlRDD.collect()
    println(result.toBuffer)
    sc.stop
  }
}
