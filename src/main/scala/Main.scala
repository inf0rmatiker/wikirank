import wikirank.PageRank
import wikirank.WikiBomb

object Main {
  
  def main(args: Array[String]): Unit = {
    val linksFiles: Array[String] = Array("hdfs://cheyenne:30241/cs435/PA3/data/links-simple-sorted.txt")
    val titlesFiles: Array[String] = Array("hdfs://cheyenne:30241/cs435/PA3/data/titles-sorted.txt")
    
    val profile = args(1).split("=")(1) 
    val iterations = 25
    val outPath = args(0) 

    profile match {
      case "0" => 
                  var pr = new PageRank("WikiRank Ideal", linksFiles, titlesFiles)
                  pr.calculatePageRank(outPath, iterations, false) 
      case "1" =>  
                  var pr = new PageRank("WikiRank Taxed", linksFiles, titlesFiles)
                  pr.calculatePageRank(outPath, iterations, true)
      case "2" =>  
                  var wb = new WikiBomb("surfing", "Rocky_Mountain_National_Park", linksFiles, titlesFiles, "WikiBomb")
                  wb.bombTarget(outPath)
      case _   => printUsageMessage() 
    }    
 
  
  }


  def printUsageMessage() {
    println("Usage: aslkdjaslkdja")

  }

  


}
