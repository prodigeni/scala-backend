package com.wikia.phalanx.tests

import au.com.bytecode.opencsv.CSVReader
import com.wikia.phalanx._
import org.scalatest._

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
	"Loaded rules " should "have 1000 rules" in {
		assert(rules.length === 1000)
	}
	it should "have first id 1" in {
		assert(rules.head.dbId === 1)
	}
	it should "have last id 4014" in {
		assert(rules.last.dbId === 4014)
	}

	def checkSystem(rs: RuleSystem) {
		intercept[RuleViolation] {
			rs.check(Checkable("fuck"))
		}
		rs.check(Checkable("something else"))
		rs.stats
	}

	val rs = new FlatRuleSystem(rules)
	"RuleSystem" should "should work Flat" in {
		checkSystem(rs)
	}
	it should "also work Combined" in {
		checkSystem(rs.combineRules)
	}
}

object DataTests {
	val curdir = System.getProperty("user.dir")
	val dir = if (curdir.endsWith("phalanx")) curdir else curdir + "/phalanx" // for tests to work when run from parent directory
	lazy val csv = new CSVFile(dir + "/src/test/resources/data1.csv").toStream.drop(1)
	// skip header p_id,p_type,p_exact,p_regex,p_case,"cast(p_text as CHAR(255))"
	lazy val rules = csv.map(arr => {
		val Array(p_id: String, p_type: String, p_exact: String, p_regex: String, p_case: String, p_text: String) = arr
		try {
			new DatabaseRule(p_text, p_id.toInt, "", p_case == "1", p_exact == "1", p_regex == "1", None, None, 0)
		}
		catch {
			case e: Throwable => {
				printf("Could not create rule number: %s\n", p_id)
				throw e
			}
		}
	}).toIndexedSeq
}
