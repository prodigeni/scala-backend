package com.wikia.phalanx.tests

import com.wikia.phalanx._
import org.scalatest._
import au.com.bytecode.opencsv.CSVReader
import com.wikia.phalanx.RuleDatabaseInfo

class CSVFile(fileName: String, charset: String = "UTF-8", separator: Char = ',', quote: Char = '"', escape: Char = '\0') extends Traversable[Array[String]] {
  override def foreach[U](f: Array[String] => U) {
    val csvReader = new CSVReader(io.Source.fromFile(fileName, charset).bufferedReader(), separator, quote, escape)
    try {
      while (true) {
        val values = csvReader.readNext()
        if (values != null) f(values)
        else return
      }
    } finally {
      csvReader.close()
    }
  }
}

class DataTests extends FlatSpec {
  val curdir = System.getProperty("user.dir")
  val csv = new CSVFile(curdir + "/src/test/resources/data1.csv").toStream.drop(1) // skip header p_id,p_type,p_exact,p_regex,p_case,"cast(p_text as CHAR(255))"
  val rules = csv.map(arr => {
      val Array(p_id:String, p_type:String, p_exact:String, p_regex:String, p_case:String, p_text:String) = arr
      val info = RuleDatabaseInfo(p_text, p_id.toInt, "")
      val ruleMaker = if (p_regex=="1") Rule.regex _ else if (p_exact=="1") Rule.exact _ else Rule.contains _
      try {
        ruleMaker(info, p_case=="1")
      }
      catch {
        case e => {
          printf("Could not create rule number: %s\n", p_id)
          throw e
        }
      }
  }).toIndexedSeq

  def time[A](msg:String, f: => A) = {
    val s = System.nanoTime
    val ret = f
    println(msg+": "+(System.nanoTime-s)/1e6+"ms")
    ret
  }

  "There" should "be 1000 rules" in { rules.length === 1000 }
  "First id" should "be 1" in { rules.head.dbInfo.dbId === 1 }
  "Last id" should "be 4014" in { rules.last.dbInfo.dbId === 4014 }

  "RuleSystem" should "work" in {
    val rs = new RuleSystem(rules)
    val crs = time("combine", { rs.combineRules })
    val regexChecker = crs.checkers(CaseSensitive)
    intercept[RuleViolation] { rs.check("fuck")}
    rs.check("something else")
    crs.check("something else")

    time("rs check", { rs.check("something else") })
    time("crs check", { crs.check("something else") })

  }








}
