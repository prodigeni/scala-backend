package com.wikia.phalanx.tests

import com.wikia.phalanx.{RuleDatabaseInfo, RuleViolation, Rule, RuleSystem}
import com.wikia.phalanx.Rule._

import org.scalatest._

class RuleTests extends FlatSpec  {

  "ExactRule" should "match only exact string" in {
    val rule = Rule.exact("Something")
    rule.check("Something else")
    intercept[RuleViolation] { rule.check("Something") }
    rule.allMatches("Something") === List(rule.dbInfo)
  }

  "ContainsRule" should "match contains" in {
    val rule = Rule.contains("Something")
    rule.check("Nothing")
    intercept[RuleViolation] { rule.check("Something") }
    intercept[RuleViolation] { rule.check("Other Something or else") }
  }

  "RuleSystem" should "combine rules" in {
    val orig = new RuleSystem(List(exact("lamb"), contains("Mary")))
    val ruleSystems = Seq(orig, orig.combineRules)
    orig.combineRules.checkers.foreach( pair => println(pair._2.regex))
    ruleSystems.foreach( rule => {
      rule.check("wolf")
      intercept[RuleViolation] { rule.check("Mary has a lamb")}
      intercept[RuleViolation] { rule.check("Mary has a wolf")}
      rule.allMatches("Mary has a lamb") === List(contains("Mary").dbInfo)
      intercept[RuleViolation] { rule.check("lamb")}
      rule.check("John has a lamb")
    })
  }

}
