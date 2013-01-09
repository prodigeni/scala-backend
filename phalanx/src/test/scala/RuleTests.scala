package com.wikia.phalanx.tests

import com.wikia.phalanx._

import org.scalatest._

class RuleTests extends FlatSpec  {

  "ExactChecker" should "match only exact string" in {
    val rule = Rule.exact("Something")
    rule.check(Checkable("Something else"))
    intercept[RuleViolation] { rule.check(Checkable("Something")) }
    assert(rule.allMatches(Checkable("Something")) === List(rule))
  }

  "ContainsChecker" should "match contains" in {
    val rule = Rule.contains("Something")
    rule.check(Checkable("Nothing"))
    intercept[RuleViolation] { rule.check(Checkable("Something")) }
    intercept[RuleViolation] { rule.check(Checkable("Other Something or else")) }
  }

  "FlatRuleSystem" should "combine rules" in {
    val orig = new FlatRuleSystem(List(Rule.exact("lamb"), Rule.contains("Mary")))
    val ruleSystems = Seq(orig, orig.combineRules)
    ruleSystems.foreach( rule => {
      rule.check(Checkable("wolf"))
      intercept[RuleViolation] { rule.check(Checkable("Mary has a lamb"))}
      intercept[RuleViolation] { rule.check(Checkable("Mary has a wolf"))}
      assert(rule.allMatches(Checkable("Mary has a lamb")) === Set(Rule.contains("Mary")))
      intercept[RuleViolation] { rule.check(Checkable("lamb"))}
      rule.check(Checkable("John has a lamb"))
    })
  }

}

