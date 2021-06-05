import org.apache.spark._
import org.apache.spark.sql.SparkSession;

object Test {
  def main(args: Array[String]): Unit = {
    /*
    val fileRead= Source.fromFile("/home/student/data/twinkle/sample.txt");
    fileRead.foreach(print);
    */
    println("bonjour hhdhd")
    val conf = new SparkConf()

    conf.setAppName("Datasets Test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    println(sc)
    val url="C:\\Users\\Mehdi Fayache\\IdeaProjects\\LIMIT_REFERENCE_MULTI.txt"
    val file = sc.textFile(url)


        val map = file.map(line =>{
          val token = line.split("\\|")
          val ID = token(0)
          val CHAMP=token(1)
          val BLOC=token(2)
          val VALEUR = token(3)
          (ID,CHAMP,BLOC,VALEUR)
        })

        map.foreach(println)


    /*

       val    fichier = sc.textFile("C:\\Users\\Mehdi Fayache\\IdeaProjects\\LIMIT_REFERENCE_MULTI.txt")
        val tableau = fichier.map(line => line.split("//|"))
        tableau.foreach(println)


              DAT_SIT,DAT_CHG,DAT_CSO

         var    champ1 = StructField("nom", StringType)
         var champ2 = StructField("prenom", StringType)
          var champ3 = StructField("age", IntType)
          schema =[champ1, champ2, champ3]
          # création d'un DataFrame sur le RDD
          multi = sqlContext.createDataFrame(tableau, schema)

      */


  }
}
