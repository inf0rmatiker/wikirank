import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Double


object App {
  
  def main(args: Array[String]): Unit = {
    //Create the spark session and the spark context
    val conf = new SparkConf().setAppName("WikiRank")
    val sc   = new SparkContext(conf)
    
    var lines  = sc.textFile("/cs435/PA3/data/wikitest.txt")

    val links = lines.map{ s =>
      val parts = s.split(": ")
      (parts(0).toInt, parts(1).split(" ").map(x => x.toInt))
    }
   /* 
    val linksCount = links.count().toInt
    var ranks = links.map(x => (x._1, 1.0/linksCount)) 
    
    for (i <- 1 to 25) {
      var c = links.join(ranks).values.flatMap{ case (urls, rank) =>
        var size = if (urls.size == 0) 1 else urls.size
        urls.map( url => (url, rank / size )) 
      }
      ranks = c.reduceByKey(_ + _)
    }
   

    ranks.coalesce(1).saveAsTextFile("/cs435/PA3/test")
   */

    links.map{ x => (x._1, x._2.mkString) }.coalesce(1).saveAsTextFile("/cs435/PA3/test")


  }

}
