package com.wikia.phalanx

import scala.collection.JavaConversions._
import com.wikia.phalanx.db.Tables.PHALANX
import com.wikia.phalanx.db.tables.records.PhalanxRecord
import java.util.regex.Pattern
import org.slf4j.{Logger, LoggerFactory}
import org.jooq.tools.unsigned.Unsigned
import collection.mutable
import java.util.Date

class RuleViolation(val rules:Traversable[DatabaseRuleInfo]) extends Exception {
  def ruleIds = rules.map( rule => rule.dbId)
}

case class Checkable(text: String, language: String = "en") {
  val lcText = text.toLowerCase
}

trait CaseType {
	def apply(s:Checkable): String
	def apply(s:String): String
}

object CaseSensitive extends CaseType {
	def apply(s:Checkable) = s.text
	def apply(s:String) = s
}

object CaseInsensitive extends CaseType {
	def apply(s:Checkable) = s.lcText
	def apply(s:String) = s.toLowerCase
}

abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	def regexPattern : String
	val caseType: CaseType
	def description(long: Boolean = false): String
}

case class ExactChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s) == text
	def regexPattern = "^" +  java.util.regex.Pattern.quote(text) + "$"
	override def toString = "ExactChecker("+text+")"
	def description(long: Boolean = false): String = "exact phrase"
}

case class ContainsChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s).contains(text)
	def regexPattern = java.util.regex.Pattern.quote(text)
	override def toString = "ContainsChecker("+text+")"
	def description(long: Boolean = false): String ="contains"
}

case class RegexChecker(caseType: CaseType, text:String) extends Checker {
	val regex = Pattern.compile(text, if (caseType == CaseSensitive) Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE else 0)
	def isMatch(s: Checkable): Boolean = regex.matcher(s.text).find() // extract not required
	def regexPattern = text
	override def toString = "RegexChecker("+text+")"
	def description(long: Boolean = false): String = if (long) "regex ("+text.length+" characters)" else "regex"
}

case class SetExactChecker(caseType: CaseType, var texts: Set[String]) extends Checker {
	if (caseType == CaseInsensitive) texts = texts.map { _.toLowerCase }
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
	def regexPattern = texts.map(java.util.regex.Pattern.quote).mkString("|")
	override def toString = "SetExactChecker("+texts.size+")"
	def description(long: Boolean = false): String ="exact set of "+texts.size+" phrases"
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
	val language: Option[String]
	val expires: Option[Date]
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
	def exact(text: String, cs:Boolean = true, lang:Option[String] = None, expires:Option[Date]=None) = new DatabaseRule(text, 0, "", cs, true, false, lang, expires)
	def regex(text: String, cs:Boolean = true, lang:Option[String] = None, expires:Option[Date]=None) = new DatabaseRule(text, 0, "", cs, false, true, lang, expires)
	def contains(text: String, cs:Boolean = true, lang:Option[String] = None, expires:Option[Date]=None) = new DatabaseRule(text, 0, "", cs, false, false, lang, expires)
}

case class DatabaseRule(text: String, dbId: Int, reason: String, caseSensitive: Boolean, exact: Boolean, regex: Boolean, language: Option[String], expires: Option[Date]) extends DatabaseRuleInfo with Rule {
	val caseType = if (caseSensitive || DatabaseRuleInfo.letterPattern.findFirstIn(text).isEmpty) CaseSensitive else CaseInsensitive
	val checker: Checker = {
		if (regex) new RegexChecker(caseType, text) else {
			if (exact) new ExactChecker(caseType, caseType(text)) else new ContainsChecker(caseType, caseType(text))
		}
	}
  def allMatches(s: Checkable): Traversable[DatabaseRuleInfo] = if (isMatch(s)) Some(this) else None
  def isMatch(s: Checkable): Boolean = (this.language.isEmpty || s.language==this.language.get ) && checker.isMatch(s)
}

trait RuleSystem extends Rule {
	def combineRules: RuleSystem
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem
	def stats: Traversable[String]
	def rules: Traversable[Rule]
	def expiring: IndexedSeq[DatabaseRuleInfo]
}

class FlatRuleSystem(initialRules: Traversable[DatabaseRule]) extends RuleSystem {
  val rules = initialRules.toSet
	val expiring = rules.filter( r => r.expires.isDefined).toIndexedSeq.sortBy( (r:DatabaseRuleInfo) => r.expires.get.getTime)
  def isMatch(s: Checkable): Boolean = rules.exists { _.isMatch(s) }
  def allMatches(s: Checkable) = rules flatMap { _.allMatches(s) }
  def combineRules: CombinedRuleSystem = new CombinedRuleSystem(rules)
	def copy(rules: Traversable[DatabaseRule]):RuleSystem = new FlatRuleSystem(rules)
  def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem = {
	  val remaining = rules.filterNot( rule => deletedIds.contains(rule.dbId) )
	  if (added.isEmpty && remaining.size == rules.size) this else copy(remaining ++ added)
  }
	protected def statsSummary(c:Checker):String = c.description(long = false)
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
		Seq( (Seq("FlatRuleSystem", "with", "total", rules.size, "rules").mkString(" "))) ++ ruleStats
	}
}

class CombinedRuleSystem(initialRules: Traversable[DatabaseRule]) extends FlatRuleSystem(initialRules) {
	val checkers:Map[Option[String], Traversable[Checker]] = extractCheckers.withDefaultValue(Set.empty)

	def extractCheckers: Map[Option[String], Traversable[Checker]] = {
		type func = Traversable[DatabaseRule] => Checker
		// using those functions as keys in the map "sets"
		val exactCs: func = texts => new SetExactChecker(CaseSensitive, texts.map(rule => rule.text).toSet)
		val exactCi: func = texts => new SetExactChecker(CaseInsensitive, texts.map(rule => rule.text).toSet)
		val cs: func = texts => new RegexChecker(CaseSensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
		val ci: func = texts => new RegexChecker(CaseInsensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
		val groupedByLang = rules.groupBy( rule => rule.language )
		def splitCase(rs:Traversable[DatabaseRule]) = {
			rs.groupBy(rule => rule.checker match {
				case c: ExactChecker => if (rule.caseType == CaseSensitive) exactCs else exactCi
				case c: Checker => if (rule.caseType == CaseSensitive) cs else ci
			}).toSeq.map(pair => pair._1(pair._2))
		}
		val result = for ( (lang, rs) <- groupedByLang ) yield (lang, splitCase(rs))
		result.toMap
	}
	override def copy(rules: Traversable[DatabaseRule]) = new CombinedRuleSystem(rules)
	override def isMatch(s: Checkable): Boolean = {
		val selected = checkers(None) ++ checkers(Some(s.language))
		selected.exists { _.isMatch(s) }
	}
	override def allMatches(s: Checkable) = if (isMatch(s)) rules flatMap { _.allMatches(s) } else Set.empty[DatabaseRuleInfo]
	override def combineRules: CombinedRuleSystem = this
	override def ruleStats: Traversable[String] = {
		val types = new mutable.HashMap[String, Set[Checker]]().withDefaultValue(Set.empty[Checker])
		for ( (lang, c) <- checkers ) {
			for ( checker <- c ) {
				val text = (if (checker.caseType == CaseSensitive) "Case sensitive" else "Case insensitive") + " (" + lang.getOrElse("All langugages") + ") : "
				types(text) = types(text) + checker
			}
		}
		types.map(  pair => {
			val (text:String, checkers:Traversable[Checker]) = pair
			text + statsPerCheckerType(checkers)
		}).toSeq.sorted
	}
	override def statsSummary(c:Checker):String = c.description(long = true)
	override def stats: Traversable[String] = {
		val checkerCount = checkers.values.map( t => t.size ).sum
		Seq( (Seq("CombinedRuleSystem", "with", "total", rules.size, "rules", "and", checkerCount, "checkers").mkString(" "))) ++ ruleStats
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
		val lang = row.getPLang
		val date = row.getPExpire

		new DatabaseRule(new String(row.getPText, "utf-8"), row.getPId.intValue(), new String(row.getPReason, "utf-8"),
			row.getPCase == 1,row.getPExact == 1, row.getPRegex == 1, if (lang == null || lang.isEmpty) None else Some(lang), com.wikia.wikifactory.DB.fromWikiTime(date))
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




