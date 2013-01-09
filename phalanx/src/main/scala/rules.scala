package com.wikia.phalanx

import scala.collection.JavaConversions._
import com.wikia.phalanx.db.Tables.PHALANX
import com.wikia.phalanx.db.tables.records.PhalanxRecord
import java.util.regex.Pattern
import org.slf4j.{Logger, LoggerFactory}
import org.jooq.tools.unsigned.Unsigned

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
	val caseType: CaseType
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

trait RuleSystem extends Rule {
	def combineRules: RuleSystem
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem
	def stats: Traversable[String]
}

class FlatRuleSystem(initialRules: Traversable[DatabaseRule]) extends RuleSystem {
  val rules = initialRules.toSet
  def isMatch(s: Checkable): Boolean = rules.exists { _.isMatch(s) }
  def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
  def combineRules: CombinedRuleSystem = new CombinedRuleSystem(rules)
	def copy(rules: Traversable[DatabaseRule]):RuleSystem = new FlatRuleSystem(rules)
  def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem = {
	  val remaining = rules.filterNot( rule => deletedIds.contains(rule.dbId) )
	  if (added.isEmpty && remaining.size == rules.size) this else copy(remaining ++ added)
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
		val cased = rules.groupBy( _.caseType).withDefaultValue(Set())
		Seq("Case sensitive: "+statsPerRuleType(cased(CaseSensitive)),
			  "Case insensitive: "+statsPerRuleType(cased(CaseInsensitive)))
	}
	def stats: Traversable[String] = {
		Seq( (Seq(this.getClass, "with", "total", rules.size, "rules").mkString(" "))) ++ ruleStats
	}
}

class CombinedRuleSystem(initialRules: Traversable[DatabaseRule]) extends FlatRuleSystem(initialRules) {
	val checkers:Traversable[Checker] = {
	// this block is there to make variables used inside temporary
		type func = Traversable[DatabaseRule] => Checker
		// using those functions as keys in the map "sets"
		val exactCs:func = texts => new SetExactChecker(CaseSensitive, texts.map(rule => rule.text).toSet)
		val exactCi:func = texts => new SetExactChecker(CaseInsensitive, texts.map(rule => rule.text).toSet)
		val cs: func = texts =>  new RegexChecker(CaseSensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
	  val ci: func = texts =>  new RegexChecker(CaseInsensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
		val sets = rules.groupBy( rule => rule.checker match {
			case c:ExactChecker => if (rule.caseType==CaseSensitive) exactCs else exactCi
			case c:Checker => if (rule.caseType==CaseSensitive) cs else ci
		})
		sets.toSeq.map( pair => pair._1(pair._2))
	}
	override def copy(rules: Traversable[DatabaseRule]) = new CombinedRuleSystem(rules)
	override def isMatch(s: Checkable): Boolean = {
		checkers.exists( checker => {  checker.isMatch(s) })
	}
	override def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
	override def combineRules: CombinedRuleSystem = this
	override def ruleStats: Traversable[String] = {
	val cased = checkers.groupBy( checker => checker.caseType).withDefaultValue(Seq())
		Seq("Case sensitive: "+statsPerCheckerType(cased(CaseSensitive)),
			  "Case insensitive: "+statsPerCheckerType(cased(CaseInsensitive)))
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

object RuleSystem {
	val logger = LoggerFactory.getLogger("FlatRuleSystem")

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
	private def ruleBuckets = (for (v <- contentTypes.values) yield (v, collection.mutable.Set.empty[DatabaseRule])).toMap
	private def dbRows(db: org.jooq.FactoryOperations, condition: Option[org.jooq.Condition]) : Seq[PhalanxRecord] = {
		logger.info("Getting database rows")
		val query = db.selectFrom(PHALANX).where(PHALANX.P_EXPIRE.isNull).or("p_expire > ?", com.wikia.wikifactory.DB.wikiCurrentTime)
		val rows = (condition match {
			case None => query
			case Some(x) => query.and(x)
		}).fetch().toIndexedSeq
		logger.info("Got "+rows+" rows")
		rows
	}
	private def createRules(rows: Seq[PhalanxRecord]) = {
		val result = ruleBuckets
		val ids = collection.mutable.Set.empty[Int]
		for (row: PhalanxRecord <- rows) {
			try {
				val rule = makeDbInfo(row)
				val t = row.getPType.intValue()
				for ( (i:Int, s:String) <- contentTypes) {
					if ((i & t) != 0) result(s) += rule
				}
				ids += rule.dbId
			}
			catch {
				case e:java.util.regex.PatternSyntaxException => None
				case e => throw e
			}
		}
		(result, ids)
	}
	def fromDatabase(db: org.jooq.FactoryOperations):Map[String,FlatRuleSystem] = {
		val (result, _)= createRules(dbRows(db, None))
		result.mapValues( rules => new CombinedRuleSystem(rules) )
	}

	def reloadSome(db: org.jooq.FactoryOperations, oldMap: Map[String, RuleSystem], changedIds: Set[Int]): Map[String, RuleSystem] = {
		if (changedIds.isEmpty) { 	// no info, let's do a full reload
			fromDatabase(db)
		} else {
			val (result, foundIds) = createRules(dbRows(db, Some(PHALANX.P_ID.in(changedIds.map { Unsigned.uint(_) }))))
			val deletedIds = changedIds.diff(foundIds)
			oldMap.map( pair => {
				val (key, rs) = pair
				(key, rs.reloadRules( result(key), deletedIds))
			})
		}
	}

}




