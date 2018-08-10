package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class HttpCodeParserTest extends FlatSpec {

  "extractHttpCode" should "Extract http code" in {
    val logParser = new LogParser
    val line = "pipe1.nyc.pipeline.com - - [01/Aug/1995:01:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"
    val url = logParser.extractByPattern(line, logParser.PATTERN_HTTP_CODE)
    assert(url == " 200 ")
  }

  "containsHttpCode" should "Check if http code exists in the log line" in {
    val logParser = new LogParser
    val line1 = "pipe1.nyc.pipeline.com - - [01/Aug/1995:00:12:37 -0400] \"GET /history/apollo/apollo-13/apollo-13-patch-small.gif\" 200 12859"

    assert(logParser.containsPattern(line1, logParser.PATTERN_HTTP_CODE))

    val line2 = "::1 - - 11/May/2015:06:44:40 -0400 \"OPTIONS * HTTP/1.0\" 001 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
    assert(!logParser.containsPattern(line2, logParser.PATTERN_HTTP_CODE))
  }
}
