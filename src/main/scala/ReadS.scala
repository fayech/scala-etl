import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import   org.apache.hadoop.hive.jdbc.HiveDriver

import org.apache.spark.sql.hive


object ReadS {

  def main(args: Array[String]): Unit = {
    val spark_conf = new SparkConf().setAppName("READ").setMaster("local[1]")
    val sc = new SparkContext(spark_conf)
    val spark_session = SparkSession.builder().config(spark_conf).getOrCreate()
    val myfile = sc.textFile("hdfs://127.0.0.1:8020/user/cloudera/admi1.txt")
    /*
    val url="C:\\Users\\Mehdi Fayache\\IdeaProjects\\LIMIT_REFERENCE_MULTI.txt"
    val file = readFromfile(url,spark_session)
     val file=spark_session.read.format("csv").option("delimiter","|").option("header","true").option("inferSchema","true").load(url)
    */


    myfile.collect().foreach(println)
    val meetdata = myfile.map(line => {
      val tokens = line.split(",")
      val id = tokens(0)
      val compat = tokens(1)
      val montant = tokens(2)
      val email = tokens(3)
      (id, compat, montant, email)
    })
    meetdata.foreach(println)
    print(meetdata)


    val dfWithSchema = spark_session.createDataFrame(meetdata).toDF("id", "compat", "montant", "email")
    /*
    dfWithSchema.show()

 */
    val date_add = udf((x: String, y: Int) => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val result = new Date(sdf.parse(x).getTime() + TimeUnit.DAYS.toMillis(y))
      sdf.format(result)
    })
    print(date_add)
    /*val newDF = dfWithSchema.withColumn("new_date", expr("date_add(current_date,days)"))

    */

    val format = new SimpleDateFormat("d-M-y")
    println(format)
    val day = java.time.LocalDate.now
    dfWithSchema.withColumn("DAT_SIT", lit(java.time.LocalDate.now.toString)).withColumn("DAT_CHG", lit(java.time.LocalDate.now.toString)).withColumn("DAT_CSO", lit("null")).show(false)

    /*
    dfWithSchema.write
      .format("rach.source.JdbcSourceV2")
      .option("url", "jdbc:hive://127.0.0.1:10000/value")
      .option("user", "cloudera")
      .option("password", "")
      .option("table", "user")
      .save()
    val driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"

    Class.forName(driverName)

    val con = DriverManager.getConnection("jdbc:hive://localhost:10000/value", "cloudera", "")
    val stmt = con.createStatement
    val tableName = "user"

    val res = stmt.executeQuery("create table " + tableName + " (id string, compat string,montant string,email string)")
    sqlContext.sql("create table yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")
   val driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"

    Class.forName(driverName)
  */

    val driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"

    Class.forName(driverName)
    dfWithSchema.write
      .format("jdbc")
      .option("url", "jdbc:hive2://127.0.0.1:10000/default")
      .option("user", "cloudera")
      .option("password", "cloudera")
      .option("dbtable", "admi1")
      .save()

  }


}
