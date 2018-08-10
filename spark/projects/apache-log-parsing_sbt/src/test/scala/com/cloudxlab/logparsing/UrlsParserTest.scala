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
  }
}
