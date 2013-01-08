package com.wikia.phalanx

import scala.collection.JavaConversions._
import com.wikia.phalanx.db.Tables.PHALANX
import com.wikia.phalanx.db.tables.records.PhalanxRecord
import scala.collection
import java.util.regex.Pattern

class RuleViolation(val rules:Traversable[DatabaseRuleInfo]) extends Exception {
  def ruleIds = rules.map( rule => rule.dbId)
}

case class Checkable(text: String) {
  val lcText = text.toLowerCase
}

trait CaseType {
	def apply(s:Checkable): String
	def apply(s:String): String
}

object CaseSensitive extends CaseType {
	def apply(s:Checkable) = s.text
	def apply(s:String) = s
	override def toString = "CaseSensitive"
}

object CaseInsensitive extends CaseType {
	def apply(s:Checkable) = s.lcText
	def apply(s:String) = s.toLowerCase
	override def toString = "CaseInsensitive"
}

abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	def regexPattern : String
}

case class ExactChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s) == text
	def regexPattern = "^" +  java.util.regex.Pattern.quote(text) + "$"
	override def toString = "ExactChecker("+text+")"
}

case class ContainsChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s).contains(text)
	def regexPattern = java.util.regex.Pattern.quote(text)
	override def toString = "ContainsChecker("+text+")"
}

case class RegexChecker(caseType: CaseType, text:String) extends Checker {
	val regex = Pattern.compile(text, if (caseType == CaseSensitive) Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE else 0)
	def isMatch(s: Checkable): Boolean = regex.matcher(s.text).find() // extract not required
	def regexPattern = text
	override def toString = "RegexChecker("+text+")"
}

case class SetExactChecker(caseType: CaseType, var texts: Set[String]) extends Checker {
	if (caseType == CaseInsensitive) texts = texts.map { _.toLowerCase }
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
	def regexPattern = texts.map(java.util.regex.Pattern.quote).mkString("|")
	override def toString = "SetExactChecker("+texts.size+")"
}


object DatabaseRuleInfo {
	// check if the text contains any letters at all
	val letterPattern = "\\p{L}".r
}

trait DatabaseRuleInfo {
	val text: String
	val dbId: Int
	val reason: String
	val caseSensitive: Boolean
	val exact: Boolean
	val regex: Boolean
}



trait Rule {
  def isMatch(s: Checkable): Boolean
  def allMatches(s: Checkable): Traversable[DatabaseRuleInfo]
  def check(s: Checkable) {
    val m = allMatches(s)
    if (m.nonEmpty) throw new RuleViolation(m)
  }
}

object Rule {
	// simple test cases
	def exact(text: String, cs:Boolean = true) = new DatabaseRule(text, 0, "", cs, true, false)
	def regex(text: String, cs:Boolean = true) = new DatabaseRule(text, 0, "", cs, false, true)
	def contains(text: String, cs:Boolean = true) = new DatabaseRule(text, 0, "", cs, false, false)
}

case class DatabaseRule(text: String, dbId: Int, reason: String, caseSensitive: Boolean, exact: Boolean, regex: Boolean) extends DatabaseRuleInfo with Rule {
	val caseType = if (caseSensitive || DatabaseRuleInfo.letterPattern.findFirstIn(text).isEmpty) CaseSensitive else CaseInsensitive
	val checker: Checker = {
		if (regex) new RegexChecker(caseType, text) else {
			if (exact) new ExactChecker(caseType, caseType(text)) else new ContainsChecker(caseType, caseType(text))
		}
	}
  def allMatches(s: Checkable): Traversable[DatabaseRuleInfo] = if (isMatch(s)) Some(this) else None
  def isMatch(s: Checkable): Boolean = checker.isMatch(s)
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

  def makeDbInfo(row:PhalanxRecord) = {
    new DatabaseRule(new String(row.getPText, "utf-8"), row.getPId.intValue(), new String(row.getPReason, "utf-8"),
	    row.getPCase == 1,row.getPExact == 1, row.getPRegex == 1)
  }

  def fromDatabase(db: org.jooq.FactoryOperations):Map[String,RuleSystem] = {
    val result = new collection.mutable.HashMap[String, collection.mutable.Set[DatabaseRule]]()
    for (v <- contentTypes.values) result(v) = collection.mutable.Set.empty[DatabaseRule]
    val query = db.selectFrom(PHALANX).where(PHALANX.P_EXPIRE.isNull) // todo: checking expire?
    for (row: PhalanxRecord <- query.fetch().iterator()) {
      try {
        val rule = makeDbInfo(row)
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
		if (!exactCs.isEmpty) result = (CaseSensitive, new SetExactChecker(CaseSensitive, exactCs.toSet)) :: result
		if (!exactCi.isEmpty) result = (CaseInsensitive, new SetExactChecker(CaseInsensitive, exactCi.toSet)) :: result
		if (!cs.isEmpty) result = (CaseSensitive, new RegexChecker(CaseSensitive, cs.mkString("|"))) :: result
		if (!ci.isEmpty) result = (CaseInsensitive, new RegexChecker(CaseInsensitive, ci.mkString("|"))) :: result
		result.toSeq
	}
  override def isMatch(s: Checkable): Boolean = {
    checkers.exists( (pair: (CaseType, Checker)) => {
	    val (caseType: CaseType, checker: Checker) = pair
	    checker.isMatch(s)
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




