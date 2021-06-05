import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col


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
    val meetdata= myfile.map(line => {
      val tokens = line.split(",")
      val id = tokens(0)
      val compat= tokens(1)
      val montant = tokens(2)
      val email = tokens(4)
      (id,compat,montant,email)
    })
    meetdata
    meetdata.foreach(println)
    print(meetdata)






  }
}
