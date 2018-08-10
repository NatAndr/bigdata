package com.cloudxlab.logparsing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LogParser extends Serializable {
  val PATTERN_IP = "^([0-9\\.]+) .*$"
  val PATTERN_URL = "^.*( /[^\" ]+).*$"
  val PATTERN_DATE = "^.*\\[([^ ]{20}).*$"
  val PATTERN_HTTP_CODE = "^.*( [1-9][0-9][0-9] ).*$"

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
    println()
    for (i <- topUrls) {
      println(i)
    }
  }
}