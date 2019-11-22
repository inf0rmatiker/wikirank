import wikirank.PageRank
import wikirank.WikiBomb

object Main {
  
  def main(args: Array[String]): Unit = {
        if (args.length < 4) {
      printUsageMessage()
    }
    else {

      val iterations: Int = 25
      val outPath:    String = args(0) 
      val profile:    String = args(1).split("=")(1) 
      val linksFile:  String = args(2).split("=")(1)
      val titlesFile: String = args(3).split("=")(1)

      profile match {
        case "0" => 
                    var pr = new PageRank("WikiRank Ideal", linksFile, titlesFile)
                    pr.calculatePageRank(outPath, iterations, false) 
        case "1" =>  
                    var pr = new PageRank("WikiRank Taxed", linksFile, titlesFile)
                    pr.calculatePageRank(outPath, iterations, true)
        case "2" =>  
                    var wb = new WikiBomb("surfing", "Rocky_Mountain_National_Park", linksFile, titlesFile, "WikiBomb")
                    wb.bombTarget(outPath)
        case _   => printUsageMessage() 
      }     
    }

  }


  def printUsageMessage() {
    println("\nUsage:\n\n\t$SPARK_HOME/bin/spark-submit  \\ \n" + 
    "\t\t--class Main \\ \n" +
    "\t\t--master spark://dover:34567 \\ \n" +
    "\t\t--executor-cores 2 \\ \n" +
    "\t\t--num-executors 4 \\ \n" +
    "\t\t--executor-memory 2g \\ \n" +
    "\t\t--driver-memory 3g \\ \n" +
    "\t\t--supervise wikirank.jar <hdfs_output_path> --profile=[0|1|2]\n")

    println("Description:\n\n" +
    "\t--profile=[option]: Option 0 specifies the idealized (untaxed) PageRank algorithm.\n" +
    "\t\t\t    Option 1 specifies the taxation-based PageRank algorithm.\n" +
    "\t\t\t    Option 2 specifies the WikiBomb example.\n")
  }

  


}
