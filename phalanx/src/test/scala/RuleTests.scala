package com.wikia.phalanx.tests

import com.wikia.phalanx.{RuleDatabaseInfo, RuleViolation, Rule, RuleSystem}
import com.wikia.phalanx.Rule

import org.scalatest._

class RuleTests extends FlatSpec  {

  "ExactChecker" should "match only exact string" in {
    val rule = Rule.exact("Something")
    rule.check("Something else")
    intercept[RuleViolation] { rule.check("Something") }
    assert(rule.allMatches("Something") === List(rule.dbInfo))
  }

  "ContainsChecker" should "match contains" in {
    val rule = Rule.contains("Something")
    rule.check("Nothing")
    intercept[RuleViolation] { rule.check("Something") }
    intercept[RuleViolation] { rule.check("Other Something or else") }
  }

  "RuleSystem" should "combine rules" in {
    val orig = new RuleSystem(List(Rule.exact("lamb"), Rule.contains("Mary")))
    val ruleSystems = Seq(orig, orig.combineRules)
    ruleSystems.foreach( rule => {
      rule.check("wolf")
      intercept[RuleViolation] { rule.check("Mary has a lamb")}
      intercept[RuleViolation] { rule.check("Mary has a wolf")}
      assert(rule.allMatches("Mary has a lamb") === Set(Rule.contains("Mary").dbInfo))
      intercept[RuleViolation] { rule.check("lamb")}
      rule.check("John has a lamb")
    })
  }

}

