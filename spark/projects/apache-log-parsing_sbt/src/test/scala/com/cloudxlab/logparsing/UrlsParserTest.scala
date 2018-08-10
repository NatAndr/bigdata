package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class UrlsParserTest extends FlatSpec {

  "extractUrl" should "Extract URL" in {
    val logParser = new LogParser
    val line = "pipe1.nyc.pipeline.com - - [01/Aug/1995:00:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"
    val url = logParser.extractByPattern(line, logParser.PATTERN_URL)
    assert(url == "/history/apollo/apollo-13/apollo-13-patch-small.gif")
  }

  "containsUrl" should "Check if URL exists in the log line" in {
    val logParser = new LogParser
    val line1 = "pipe1.nyc.pipeline.com - - [01/Aug/1995:00:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"

    assert(logParser.containsPattern(line1, logParser.PATTERN_URL))

    val line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
    assert(!logParser.containsPattern(line2, logParser.PATTERN_URL))

    val line3 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    assert(logParser.containsPattern(line3, logParser.PATTERN_URL))

  }
}
