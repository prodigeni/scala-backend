package com.wikia.phalanx

class DifferencesPage(rules: Seq[DatabaseRuleInfo]) {
  val printer = new scala.xml.PrettyPrinter(160, 2)

  lazy val head = s"""<html lang="en">
<head><title>Phalanx - Differences summary page</title></head>
<body>
<h2>Differences detected between old and new Phalanx (${rules.size} rules):</h2>
<ol>
"""
  val tail = "\n</ol></body></html>"

  def specialPageLink(id:Int) = s"http://communitytest.wikia.com/wiki/Special:Phalanx?id=$id"
  def formatRule(rule: DatabaseRuleInfo)= <li><a href={specialPageLink(rule.dbId)} target="phalanx_rule">Rule #{rule.dbId}: {rule.text}</a></li>

  lazy val response = Respond(contentType = "text/html; charset=UTF-8", content = Seq(head, printer.formatNodes(rules.map(formatRule)), tail).mkString)
}
