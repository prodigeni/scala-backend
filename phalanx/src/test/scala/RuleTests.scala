package com.wikia.phalanx.tests

import com.wikia.phalanx._
import org.scalatest._

class RuleTests extends FlatSpec {

	"Checkers" should "match exact string" in {
		val rule = Rule.exact("Something")
		rule.check(Checkable("Something else"))
		intercept[RuleViolation] {
			rule.check(Checkable("Something"))
		}
		assert(rule.allMatches(Checkable("Something")) === List(rule))
	}

	it should "match contains" in {
		val rule = Rule.contains("Something")
		rule.check(Checkable("Nothing"))
		intercept[RuleViolation] {
			rule.check(Checkable("Something"))
		}
		intercept[RuleViolation] {
			rule.check(Checkable("Other Something or else"))
		}
	}
	val orig = new FlatRuleSystem(List(Rule.exact("lamb"), Rule.contains("Mary"), Rule.contains("scheisse", false, Some("de"))))
	def checkSystem(rule: RuleSystem) {
		rule.check(Checkable("wolf"))
		intercept[RuleViolation] {
			rule.check(Checkable("Mary has a lamb"))
		}
		intercept[RuleViolation] {
			rule.check(Checkable("Mary has a wolf"))
		}
		assert(rule.allMatches(Checkable("Mary has a lamb")).toSet === Set(Rule.contains("Mary")))
		intercept[RuleViolation] {
			rule.check(Checkable("lamb"))
		}
		rule.check(Checkable("John has a lamb"))
		rule.check(Checkable("scheisse in english should work", "en"))
		intercept[RuleViolation] {
			rule.check(Checkable("scheisse in german shouldn't", "de"))
		}
	}

	"RuleSystem" should "work flat" in {
		checkSystem(orig)
	}
	it should "work combined" in {
		checkSystem(orig.combineRules)
	}

}

