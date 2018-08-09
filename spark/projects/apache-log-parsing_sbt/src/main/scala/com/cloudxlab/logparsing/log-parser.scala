package com.cloudxlab.logparsing

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, _}

class Utils extends Serializable {
  val PATTERN_IP = "^([0-9\\.]+) .*$"
  val PATTERN_URL = "^.*GET ([^\" ]*).*$"

  def containsPattern(line: String, pattern: String): Boolean = line matches pattern

  def extractByPattern(line: String, p: String): String = {
    val pattern = p.r
    val pattern(s: String) = line
    s
  }

  def getTopNByPattern(accessLogs: RDD[String], sc: SparkContext, num: Int, pattern: String, sortAscending: Boolean): Array[(String, Int)] = {
    val logs = accessLogs.filter(containsPattern(_, pattern))
    val extracted = logs.map(extractByPattern(_, pattern))
    val tuples = extracted.map((_, 1))
    val frequencies = tuples.reduceByKey(_ + _)
    val sortedFrequencies = frequencies.sortBy(x => x._2, sortAscending)

    accessLogs.filter(_.contains("POST")).take(10)

    sortedFrequencies.take(num)
  }
}

object EntryPoint {
  val usage =
    """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Expected: 3 , Provided: " + args.length)
      println(usage)
      return
    }

    val utils = new Utils

    // Create a local StreamingContext with batch interval of 10 second
    val conf = new SparkConf().setAppName("Spark Project - Log parsing")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
    val logs = sc.textFile(args(2))
    val num = args(1).toInt
    val top10Urls = utils.getTopNByPattern(logs, sc, num, utils.PATTERN_URL, sortAscending = false)
    println("===== TOP 10 URLs =====")
    for (i <- top10Urls) {
      println(i)
    }
  }
}
