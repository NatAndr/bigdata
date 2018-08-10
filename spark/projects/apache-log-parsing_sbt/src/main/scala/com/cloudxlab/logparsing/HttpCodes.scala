package com.cloudxlab.logparsing

import org.apache.spark.{SparkConf, SparkContext}

object HttpCodes {
  val usage =
    """
        Usage: HttpCodes <how_many> <file_or_directory_in_hdfs>
        Example: HttpCodes 10 /data/spark/project/access/access.log.45.gz
    """

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Expected: 3 , Provided: " + args.length)
      println(usage)
      return
    }

    val logParser = new LogParser

    // Create a local StreamingContext with batch interval of 10 second
    val conf = new SparkConf().setAppName("Spark Project - Log parsing")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val logs = sc.textFile(args(2))
    val num = args(1).toInt
    val fx = (x: String) => x.trim

    val topHttpCodes = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_HTTP_CODE, sortAscending = false, fx)
    logParser.printTop(topHttpCodes, "http codes", num)
  }
}
