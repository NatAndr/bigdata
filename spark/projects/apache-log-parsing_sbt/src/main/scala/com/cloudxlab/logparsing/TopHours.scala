package com.cloudxlab.logparsing

import org.apache.spark.{SparkConf, SparkContext}

object TopHours {
  val usage =
    """
        Usage: TopHours <how_many> <file_or_directory_in_hdfs>
        Example: TopHours 10 /data/spark/project/access/access.log.45.gz
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
    val fx = (x: String) => x.substring(12, 14)

    val topHoursWithHighTraffic = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_DATE, sortAscending = false, fx)
    logParser.printTop(topHoursWithHighTraffic, "hours with high traffic", num)

    val topHoursWithLowTraffic = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_DATE, sortAscending = true, fx)
    logParser.printTop(topHoursWithLowTraffic, "hours with low traffic", num)
  }
}
