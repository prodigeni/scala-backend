package com.wikia.phalanx

class RuleViolation(val rules:Traversable[RuleDatabaseInfo]) extends Exception {
  def ruleIds = rules.map( rule => rule.dbId)
}

case class Checkable(text: String) {
  val lcText = text.toLowerCase
}

case class RuleDatabaseInfo(text: String, dbId: Int, reason: String) {

}

trait CaseType {
  def extract(s:Checkable): String
  def extract(s:String): String
}

object CaseSensitive extends CaseType {
  def extract(s:Checkable) = s.text
  def extract(s:String) = s
  override def toString = "CaseSensitive"
}

object CaseInsensitive extends CaseType {
  def extract(s:Checkable) = s.lcText
  def extract(s:String) = s.toLowerCase
  override def toString = "CaseInsensitive"
}

abstract class Checker(val text: String) {
  def isMatch(s: String): Boolean
  def regexPattern : String
}

class ExactChecker(text: String) extends Checker(text) {
  def isMatch(s: String): Boolean = s == text
  def regexPattern = "^" +  java.util.regex.Pattern.quote(text) + "$"
  override def toString = "ExactChecker("+text+")"

}

class ContainsChecker(text: String) extends Checker(text) {
  def isMatch(s: String): Boolean = s.contains(text)
  def regexPattern = java.util.regex.Pattern.quote(text)
  override def toString = "ContainsChecker("+text+")"
}

class RegexChecker(text:String) extends Checker(text) {
  val regex = text.r
  def isMatch(s: String): Boolean = regex.findFirstIn(s).isDefined
  def regexPattern = text
  override def toString = "RegexChecker("+regex+")"
}

trait Rule {
  def isMatch(s: Checkable): Boolean
  def allMatches(s: Checkable): Traversable[RuleDatabaseInfo]
  def check(s: Checkable) {
    val m = allMatches(s)
    if (m.nonEmpty) throw new RuleViolation(m)
  }
}

class DatabaseRule(val dbInfo: RuleDatabaseInfo, val caseType: CaseType, checkerFactory: String => Checker) extends Rule {
  val checker = checkerFactory(caseType.extract(dbInfo.text))
  def allMatches(s: Checkable): Traversable[RuleDatabaseInfo] = if (isMatch(s)) Some(dbInfo) else None
  def this(s: String, caseType: CaseType, checkerFactory: String => Checker) = this(new RuleDatabaseInfo(s, 0, ""), caseType, checkerFactory)
  def isMatch(s: Checkable): Boolean = checker.isMatch(caseType.extract(s))
}

class RuleSystem(initialRules: Traversable[DatabaseRule]) extends Rule {
  val rules = initialRules.toSet
  def isMatch(s: Checkable): Boolean = rules.exists { _.isMatch(s) }
  def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
  def combineRules: CombinedRuleSystem = new CombinedRuleSystem(rules)
  def reloadRules(added: Iterable[DatabaseRule], deletedIds: Iterable[Int]): Rule = {
    this // todo
  }
}

class CombinedRuleSystem(initialRules: Traversable[DatabaseRule]) extends RuleSystem(initialRules) {
  val checkers = Map( (CaseSensitive, new RegexChecker(rules.map { _.checker.regexPattern }.mkString("|"))),
                      (CaseInsensitive, new RegexChecker(rules.map { _.checker.regexPattern }.mkString("|"))) )
  override def isMatch(s: Checkable): Boolean = {
    checkers.exists( (pair: (CaseType, RegexChecker)) => {
      pair match {
        case (caseType: CaseType, rule: Rule) => rule.isMatch(caseType.extract(s))
      }
    })
  }
  override def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
  override def combineRules: CombinedRuleSystem = this
}

object Rule {
  def apply(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean, checkerFactory: String => Checker) = {
    new DatabaseRule(dbInfo, if (caseSensitive) CaseSensitive else CaseInsensitive, checkerFactory)
  }
  def regex(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive, s => new RegexChecker(s))
  def exact(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive, s => new ExactChecker(s))
  def contains(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive, s => new ContainsChecker(s))

}



