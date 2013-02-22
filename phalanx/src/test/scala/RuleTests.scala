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
	val testRule = new DatabaseRule(text = "Szumo ma kota5", dbId = 53718, reason = "", caseSensitive = false, exact = false, regex = false,
		language = None, authorId = 5428556, typeMask = 5, expires = None )
	val replacementRule = new DatabaseRule(text = "Szumo ma kota6", dbId = 53718, reason = "", caseSensitive = false, exact = false, regex = false,
		language = None, authorId = 5428556, typeMask = 5, expires = None )
	val orig = new FlatRuleSystem(List(Rule.exact("lamb"), Rule.contains("Mary"), Rule.contains("scheisse", false, Some("de")), testRule))
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
		intercept[RuleViolation] {
			rule.check(Checkable("Szumo ma kota5", "en"))
		}
		assert(rule.allMatches(Checkable("Szumo ma kota5", "en")).toSet === Set(testRule))
		val newRs = rule.reloadRules(Seq(replacementRule), Seq.empty)
		newRs.check(Checkable("Szumo ma kota5", "en"))
		intercept[RuleViolation] {
			newRs.check(Checkable("Szumo ma kota6", "en"))
		}
		assert(newRs.allMatches(Checkable("Szumo ma kota5", "en")).toSet === Set.empty)
		assert(newRs.allMatches(Checkable("Szumo ma kota6", "en")).toSet === Set(replacementRule))
	}

	"RuleSystem" should "work flat" in {
		checkSystem(orig)
	}
	it should "work combined" in {
		checkSystem(orig.combineRules)
	}
}

