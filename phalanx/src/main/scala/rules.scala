package com.wikia.phalanx

import collection.JavaConversions._
import com.wikia.phalanx.db.Tables.PHALANX
import com.wikia.phalanx.db.tables.records.PhalanxRecord
import collection.parallel.mutable
import scala.collection
import scala.collection

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
  override def toString = "CS"
}

object CaseInsensitive extends CaseType {
  def extract(s:Checkable) = s.lcText
  def extract(s:String) = s.toLowerCase
  override def toString = "CI"
}

abstract sealed class Checker {
  def isMatch(s: String): Boolean
  def regexPattern : String
}

case class ExactChecker(text: String) extends Checker {
  def isMatch(s: String): Boolean = s == text
  def regexPattern = "^" +  java.util.regex.Pattern.quote(text) + "$"
  override def toString = "ExactChecker("+text+")"
}

case class ContainsChecker(text: String) extends Checker {
  def isMatch(s: String): Boolean = s.contains(text)
  def regexPattern = java.util.regex.Pattern.quote(text)
  override def toString = "ContainsChecker("+text+")"
}

case class RegexChecker(text:String) extends Checker {
  val regex = text.r
  def isMatch(s: String): Boolean = regex.findFirstIn(s).isDefined
  def regexPattern = text
  override def toString = "RegexChecker("+regex+")"
}

case class SetExactChecker(texts: Set[String]) extends Checker {
	def isMatch(s: String): Boolean = texts.contains(s)
	def regexPattern = texts.map(java.util.regex.Pattern.quote).mkString("|")
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
	protected def statsSummary(c:Checker):String = {
		c match {
		case _:ExactChecker => "exact phrase"
		case c:RegexChecker => "regex"
		case _:ContainsChecker => "contains"
		case c:SetExactChecker => "exact set of "+c.texts.size+" phrases"
		}
	}
	protected def statsPerCheckerType(rs: Traversable[Checker]):String = {
		val groups = rs.groupBy( c =>  statsSummary(c)).toIndexedSeq.sortBy( (pair: (String, Traversable[Checker])) => pair._1)
		groups.map( (pair : (String, Traversable[Checker])) => pair._1 + (if (pair._2.size>1) { "=" + pair._2.size } else { "" } ) ).mkString(", ")
	}
	protected def statsPerRuleType(rs: Traversable[DatabaseRule]):String = statsPerCheckerType(rs.map(rule => rule.checker))
	def ruleStats: Traversable[String] = {
		val cased = rules.groupBy( _.caseType)
		Seq("Case sensitive: "+statsPerRuleType(cased.getOrElse(CaseSensitive, Set())),
			  "Case insensitive: "+statsPerRuleType(cased.getOrElse(CaseInsensitive, Set())))
	}
	def stats: Traversable[String] = {
		Seq( (Seq(this.getClass, "with", "total", rules.size, "rules").mkString(" "))) ++ ruleStats
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
    result.mapValues( rules => new RuleSystem(rules).combineRules ).toMap
  }
}

class CombinedRuleSystem(initialRules: Traversable[DatabaseRule]) extends RuleSystem(initialRules) {
	val checkers:Seq[(CaseType, Checker)] = { // this block is there to make variables used inside temporary
		val exactCs = new collection.mutable.HashSet[String]
		val exactCi = new collection.mutable.HashSet[String]
		val cs = new collection.mutable.HashSet[String]
		val ci = new collection.mutable.HashSet[String]
		rules.foreach( rule => rule.checker match {
			case c:ExactChecker => if (rule.caseType==CaseSensitive) { exactCs += c.text } else { exactCi += c.text }
			case c:Checker => if (rule.caseType==CaseSensitive) { cs += c.regexPattern } else { ci += c.regexPattern }
		})
		var result = List[ (CaseType, Checker) ]()
		if (!exactCs.isEmpty) result = (CaseSensitive, new SetExactChecker(exactCs.toSet)) :: result
		if (!exactCi.isEmpty) result = (CaseInsensitive, new SetExactChecker(exactCi.toSet)) :: result
		if (!cs.isEmpty) result = (CaseSensitive, new RegexChecker(cs.mkString("|"))) :: result
		if (!ci.isEmpty) result = (CaseInsensitive, new RegexChecker(cs.mkString("|"))) :: result
		result.toSeq
	}
  override def isMatch(s: Checkable): Boolean = {
    checkers.exists( (pair: (CaseType, Checker)) => {
	    val (caseType: CaseType, checker: Checker) = pair
	    checker.isMatch(caseType.extract(s))
    })
  }
  override def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
  override def combineRules: CombinedRuleSystem = this
	override def ruleStats: Traversable[String] = {
		val cased = checkers.groupBy( _._1)
		Seq("Case sensitive: "+statsPerCheckerType(cased.getOrElse(CaseSensitive, Seq()).map( pair => pair._2)),
			"Case insensitive: "+statsPerCheckerType(cased.getOrElse(CaseInsensitive, Seq()).map( pair => pair._2)))
	}
	override def statsSummary(c:Checker):String = {
		c match {
			case _:ExactChecker => "exact phrase"
			case c:RegexChecker => "regex ("+c.text.length+" characters)"
			case _:ContainsChecker => "contains"
			case c:SetExactChecker => "exact set of "+c.texts.size+" phrases"
		}
	}
}

object Rule {
  def apply(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean, checkerFactory: String => Checker) = {
    new DatabaseRule(dbInfo, if (caseSensitive) CaseSensitive else CaseInsensitive, checkerFactory)
  }
  def regex(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive || caseIrrelevant(dbInfo), s => new RegexChecker(s))
  def exact(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive || caseIrrelevant(dbInfo), s => new ExactChecker(s))
  def contains(dbInfo: RuleDatabaseInfo, caseSensitive: Boolean = false) = this(dbInfo, caseSensitive || caseIrrelevant(dbInfo), s => new ContainsChecker(s))
	// check if the text contains any letters at all
	private val letterPattern = "\\p{L}".r
	def caseIrrelevant(dbInfo: RuleDatabaseInfo):Boolean = letterPattern.findFirstIn(dbInfo.text).isEmpty
}



