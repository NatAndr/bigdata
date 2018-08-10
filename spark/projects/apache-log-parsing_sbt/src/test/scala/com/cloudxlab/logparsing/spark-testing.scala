package com.cloudxlab.logparsing

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class SampleTest extends FunSuite with SharedSparkContext {
  test("Computing top10 IP") {
    val line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    val line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""

    val logParser = new LogParser

    val list = List(line1, line2)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)

    val records = logParser.getTopNByPattern(rdd, sc, 10, logParser.PATTERN_IP, sortAscending = false, null)
    assert(records.length === 1)
    assert(records(0)._1 == "121.242.40.10")
  }

  test("Computing top10 URL") {
    val line1 = "pipe1.nyc.pipeline.com - - [01/Aug/1995:00:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"
    val line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""

    val logParser = new LogParser

    val list = List(line1, line2)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)

    val records = logParser.getTopNByPattern(rdd, sc, 10, logParser.PATTERN_URL, sortAscending = false, null)
    assert(records.length === 1)
    assert(records(0)._1 == "/history/apollo/apollo-13/apollo-13-patch-small.gif")
  }

  test("Computing top5 highest traffic hours") {
    val line1 = "pipe1.nyc.pipeline.com - - [01/Aug/1995:01:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"
    val line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
    val line3 = "pipe1.nyc.pipeline.com - - [01/Aug/1995:01:13:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"

    val logParser = new LogParser

    val list = List(line1, line2, line3)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)

    val fx = (x: String) => x.substring(12, 14)
    val records = logParser.getTopNByPattern(rdd, sc, 5, logParser.PATTERN_DATE, sortAscending = false, fx)
    assert(records.length === 2)
    assert(records(0)._1 == "01")
    assert(records(1)._1 == "06")
  }
}
