package wikirank

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.lang.Double


class WikiBomb (termI: String, targetI: String, iLinks: Array[String], iTitles: Array[String], name: String) {
  var term = termI.toLowerCase()
  var target = targetI
  var inputLinks = iLinks(0) 
  var inputTitles = iTitles(0)
  
  var conf: SparkConf    = new SparkConf().setAppName(name)   
  var sc  : SparkContext = new SparkContext(conf)

  def bombTarget(outputPath: String) {
    var lines  = sc.textFile(inputLinks)
    var titles = sc.textFile(inputTitles)

    var zippedTitles = titles.zipWithIndex.map( x => (x._2.toInt + 1, x._1) ).persist()
    val links = lines.map{ s =>
        val parts = s.split(": ")
        (parts(0).toInt, parts(1).split(" ").drop(0).map(x => x.toInt))
    }.cache()

    val targetTitleEntry = zippedTitles.filter( entry => entry._2 == "Rocky_Mountain_National_Park" )
    val targetId = targetTitleEntry.take(1)(0)._1
    val targetLink = links.filter(x => x._1 == targetId).map(x => (x._1, x._2 :+ targetId))
    
    var surfTitles = zippedTitles.filter( x => x._2.toLowerCase().contains("surfing"))
    var unionTitles = surfTitles.union(targetTitleEntry)

   
    var surfIds = surfTitles.filter( x => x._2.toLowerCase()
                                          .contains("surfing")).map(x => x._1).collect()
     
    val surfLinks = links.filter{ link => surfIds.contains(link._1) }.map{ case (id, links) =>
                                        (id, links.filter{ link => surfIds.contains(link) } ) }

    val bombedLinks = surfLinks.map{ entry => ( entry._1, entry._2 :+ targetId ) }
    val fixedTargetLink = targetLink.map{ case (id, links) => 
                                (id, links.filter{ link => surfIds.contains(link) } ) }.map(x => (x._1, x._2 :+ targetId) )
    val unionLinks = fixedTargetLink.union(bombedLinks)
      
    val linksCount = unionLinks.count().toInt
    var ranks = unionLinks.mapValues(v => 1.0/linksCount)
  
    for (i <- 1 to 25) {
      var c = unionLinks.join(ranks).values.flatMap{ case (urls, rank) =>
        var size = if (urls.size == 0) 1 else urls.size
        urls.map( url => (url, rank / size )) 
      }
      ranks = c.reduceByKey(_ + _)
    }
    
    val sorted      = ranks.sortBy(_._2, false, 1).take(10)
    val toRdd       = sc.parallelize(sorted, 1).join(unionTitles).map{ case (id, (rank, name)) => (id, name, rank) }
    val sortedFinal = toRdd.sortBy(_._3, false, 1).coalesce(1)
    
    sortedFinal.saveAsTextFile(outputPath)
  }



}
