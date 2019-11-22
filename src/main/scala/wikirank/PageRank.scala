package wikirank

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Double


class PageRank (appName: String, linksIn: String, titlesIn: String) {
    val name       : String = appName
    val linksPath  : String = linksIn
    val titlesPath : String = titlesIn

    var conf: SparkConf    = new SparkConf().setAppName(name)   
    var sc  : SparkContext = new SparkContext(conf)

    def calculatePageRank(outputPath: String, iterations: Int, useTaxation: Boolean) {
      val lines = sc.textFile(linksPath)
      val titles = sc.textFile(titlesPath)

      val zippedTitles = titles.zipWithIndex.map( x => (x._2.toInt + 1, x._1) )
      val links = lines.map{ s =>
        val parts = s.split(": ")
        (parts(0).toInt, parts(1).split(" ").drop(0).map(x => x.toInt))
      }

      val linksCount = links.count().toInt
      var ranks = links.mapValues(v => 1.0/linksCount)
      
      for (i <- 1 to iterations) {
        val c = links.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
        }
        ranks = if (useTaxation) c.reduceByKey(_ + _).mapValues(0.15 / linksCount + 0.85 * _) else c.reduceByKey(_ + _)
      }
      
      val sorted      = ranks.sortBy(_._2, false, 1).take(10)
      val toRdd       = sc.parallelize(sorted, 1).join(zippedTitles).map{ case (id, (rank, name)) => (id, name, rank) }
      val sortedFinal = toRdd.sortBy(_._3, false, 1).coalesce(1)
      
      
      sortedFinal.saveAsTextFile(outputPath)
    }
    
    



}
