package Demo06JDBCRdd

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/**
 * @ClassName MyJDBCRdd
 * @MethodDesc: 使用JDBC操作数据库
 * @Author Movle
 * @Date 11/11/20 6:23 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/
object MyJDBCRdd {

  val connection = () =>{
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://192.168.31.121:3306/company?serverTimezone=UTC&characterEncoding=utf-8", "root", "000000")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("jdbc demo").setMaster("local")
    val sc = new SparkContext(conf)


    val mysqlRDD = new JdbcRDD(sc,connection,"select * from emp where sal > ? and sal <= ?",0,3000, 2, r=>{

      val ename = r.getString(2)
      val sal = r.getInt(4)

      (0,0)
    })

    val result = mysqlRDD.collect()

    mysqlRDD.foreach(println)
    //result.foreach(println)
    //println(result.toBuffer)

    sc.stop()
  }
}
