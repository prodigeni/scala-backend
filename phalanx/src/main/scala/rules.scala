package com.wikia.phalanx

import collection.JavaConversions._
import collection.{mutable, BitSet}
import com.wikia.phalanx.db.Tables.PHALANX
import com.wikia.phalanx.db.tables.records.PhalanxRecord

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

object RuleSystem {

  val contentTypes = Map(  // bitmask to types
    (1, "content"),    // const TYPE_CONTENT = 1;
    (2, "summary"),    // const TYPE_SUMMARY = 2;
    (4, "title"),      // const TYPE_TITLE = 4;
    (8, "user"),  // const TYPE_USER = 8;
    (16, "question_title"),  // const TYPE_ANSWERS_QUESTION_TITLE = 16;
    (32, "recent_questions"),  // const TYPE_ANSWERS_RECENT_QUESTIONS = 32;
    (64, "wiki_creation"),  // const TYPE_WIKI_CREATION = 64;
    (128, "cookie"),  // const TYPE_COOKIE = 128;
    (256, "email")  // const TYPE_EMAIL = 256;
  )

  def makeDbInfo(row:PhalanxRecord):RuleDatabaseInfo = {
    RuleDatabaseInfo(new String(row.getPText, "utf-8"), row.getPId.intValue(), new String(row.getPReason, "utf-8"))
  }

  def fromDatabase(db: org.jooq.FactoryOperations):Map[String,RuleSystem] = {
    val result = new collection.mutable.HashMap[String, collection.mutable.Set[DatabaseRule]]()
    for (v <- contentTypes.values) result(v) = collection.mutable.Set.empty[DatabaseRule]
    val query = db.selectFrom(PHALANX).where(PHALANX.P_EXPIRE.isNull)
    for (row: PhalanxRecord <- query.fetch().iterator()) {
      val ruleMaker = if (row.getPRegex==1) Rule.regex _ else if (row.getPExact==1) Rule.exact _ else Rule.contains _
      try {
        val rule = ruleMaker(makeDbInfo(row), row.getPCase == 1)
        val t = row.getPType.intValue()
        for ( (i:Int, s:String) <- contentTypes) {
          if ((i & t) != 0) result(s) += rule
        }
      }
      catch {
        case e:java.util.regex.PatternSyntaxException => Unit
        case e => throw e
      }
    }
    result.mapValues( rules => new CombinedRuleSystem(rules) ).toMap
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



