package com.cloudxlab.logparsing

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, _}

class LogParser extends Serializable {
  val PATTERN_IP = "^([0-9\\.]+) .*$"
  val PATTERN_URL = "^.*GET ([^\" ]*).*$"
  val PATTERN_DATE = "^.*\\[([^ ]*).*$"

  def containsPattern(line: String, pattern: String): Boolean = line matches pattern

  def extractByPattern(line: String, p: String): String = {
    val pattern = p.r
    val pattern(s: String) = line
    s
  }

  def getTopNByPattern(accessLogs: RDD[String], sc: SparkContext, num: Int, pattern: String,
                       sortAscending: Boolean, fx: String => String): Array[(String, Int)] = {
    val logs = accessLogs.filter(containsPattern(_, pattern))
    var extracted = logs.map(extractByPattern(_, pattern))
    if (fx != null) {
      extracted = extracted.map(fx)
    }
    val tuples = extracted.map((_, 1))
    val frequencies = tuples.reduceByKey(_ + _)
    val sortedFrequencies = frequencies.sortBy(x => x._2, sortAscending)

    sortedFrequencies.take(num)
  }

  def printTop(topUrls: Array[(String, Int)], name: String, num: Int): Unit = {
    printf("===== TOP %d %s =====", num, name)
    for (i <- topUrls) {
      println(i)
    }
  }
}

object TopUrls {
  val usage =
    """
        Usage: TopUrls <how_many> <file_or_directory_in_hdfs>
        Example: TopUrls 10 /data/spark/project/access/access.log.45.gz
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
    val topUrls = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_URL, sortAscending = false, null)

    logParser.printTop(topUrls, "URLs", num)
  }
}

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

    val top5HoursWithHighTraffic = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_URL, sortAscending = false, fx)
    logParser.printTop(top5HoursWithHighTraffic, "hours with high traffic", num)

    val top5HoursWithLowTraffic = logParser.getTopNByPattern(logs, sc, num, logParser.PATTERN_URL, sortAscending = true, fx)
    logParser.printTop(top5HoursWithLowTraffic, "hours with low traffic", num)
  }

}