package com.wikia.phalanx.tests

import com.wikia.phalanx._
import org.scalatest._
import au.com.bytecode.opencsv.CSVReader
import com.wikia.phalanx.DatabaseRuleInfo

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
	val rules = DataTests.rules
  "There" should "be 1000 rules" in { assert(rules.length === 1000) }
  "First id" should "be 1" in { assert(rules.head.dbId === 1) }
  "Last id" should "be 4014" in { assert(rules.last.dbId === 4014) }

  "RuleSystem" should "work" in {
    val rs = new RuleSystem(rules)
    val crs =  rs.combineRules
    intercept[RuleViolation] { rs.check(Checkable("fuck"))}
    rs.check(Checkable("something else"))
    crs.check(Checkable("something else"))


  }
}

object DataTests {
	val curdir = System.getProperty("user.dir")
	lazy val csv = new CSVFile(curdir + "/src/test/resources/data1.csv").toStream.drop(1) // skip header p_id,p_type,p_exact,p_regex,p_case,"cast(p_text as CHAR(255))"
	lazy val rules = csv.map(arr => {
			val Array(p_id:String, p_type:String, p_exact:String, p_regex:String, p_case:String, p_text:String) = arr
			try {
				new DatabaseRule(p_text,  p_id.toInt, "", p_case == "1", p_exact == "1", p_regex == "1")
			}
			catch {
				case e => {
					printf("Could not create rule number: %s\n", p_id)
					throw e
				}
			}
		}).toIndexedSeq
}
