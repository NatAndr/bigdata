package com.cloudxlab.logparsing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LogParser extends Serializable {
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
