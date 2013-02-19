package com.wikia.phalanx

import collection.mutable
import java.util.Date
import java.util.regex.Pattern
import util.parsing.json.JSONObject

class RuleViolation(val rules: Iterable[DatabaseRuleInfo]) extends Exception {
	def ruleIds = rules.map(rule => rule.dbId)
}

case class Checkable(text: String, language: String = "en") {
	val lcText = text.toLowerCase
}

trait CaseType {
	def apply(s: Checkable): String
	def apply(s: String): String
}

object CaseSensitive extends CaseType {
	def apply(s: Checkable) = s.text
	def apply(s: String) = s
}

object CaseInsensitive extends CaseType {
	def apply(s: Checkable) = s.lcText
	def apply(s: String) = s.toLowerCase
}

abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	def regexPattern: String
	val caseType: CaseType
	def description(long: Boolean = false): String
}

case class ExactChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s) == text
	def regexPattern = "^" + java.util.regex.Pattern.quote(text) + "$"
	override def toString = "ExactChecker(" + text + ")"
	def description(long: Boolean = false): String = "exact phrase"
}

case class ContainsChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s).contains(text)
	def regexPattern = java.util.regex.Pattern.quote(text)
	override def toString = "ContainsChecker(" + text + ")"
	def description(long: Boolean = false): String = "contains"
}

case class RegexChecker(caseType: CaseType, text: String) extends Checker {
	val regex = Pattern.compile(text, if (caseType == CaseSensitive) Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE else 0)
	def isMatch(s: Checkable): Boolean = regex.matcher(s.text).find()
	// extract not required
	def regexPattern = text
	override def toString = "RegexChecker(" + text + ")"
	def description(long: Boolean = false): String = if (long) "regex (" + text.length + " characters)" else "regex"
}

case class SetExactChecker(caseType: CaseType, origTexts: Iterable[String]) extends Checker {
	val texts = if (caseType == CaseInsensitive) origTexts.toSet else origTexts.map(s => s.toLowerCase).toSet
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
	def regexPattern = texts.map(java.util.regex.Pattern.quote).mkString("|")
	override def toString = "SetExactChecker(" + texts.size + ")"
	def description(long: Boolean = false): String = "exact set of " + texts.size + " phrases"
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
	val authorId: Int
	val typeMask: Int
	def toJSONObject:JSONObject = {
		JSONObject(Map(
			("id", dbId),
			("text", text),
			("type", typeMask),
			("reason", reason),
		  ("caseSensitive", caseSensitive),
		  ("exact", exact),
		  ("regex", regex),
		  ("language", language.getOrElse("")),
		  ("authorId", authorId),
		  ("expires", expires match {
			  case None => ""
				case Some(date) => com.wikia.wikifactory.DB.toWikiTime(date)
		  })
		))
	}
}

trait Rule {
	def isMatch(s: Checkable): Boolean
	def allMatches(s: Checkable): Iterable[DatabaseRuleInfo]
	def check(s: Checkable) {
		val m = allMatches(s)
		if (m.nonEmpty) throw new RuleViolation(m)
	}
}

object Rule {
	// simple test cases
	def exact(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Date] = None) = new DatabaseRule(text, 0, "", cs, true, false, lang, expires, 0, 0)
	def regex(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Date] = None) = new DatabaseRule(text, 0, "", cs, false, true, lang, expires, 0, 0)
	def contains(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Date] = None) = new DatabaseRule(text, 0, "", cs, false, false, lang, expires, 0, 0)
}

case class DatabaseRule(text: String, dbId: Int, reason: String, caseSensitive: Boolean, exact: Boolean, regex: Boolean,
                        language: Option[String], expires: Option[Date], authorId: Int, typeMask: Int) extends DatabaseRuleInfo with Rule {
	val caseType = if (caseSensitive || DatabaseRuleInfo.letterPattern.findFirstIn(text).isEmpty) CaseSensitive else CaseInsensitive
	val checker: Checker = {
		if (regex) new RegexChecker(caseType, text)
		else {
			if (exact) new ExactChecker(caseType, caseType(text)) else new ContainsChecker(caseType, caseType(text))
		}
	}
	def allMatches(s: Checkable): Iterable[DatabaseRuleInfo] = if (isMatch(s)) Some(this) else None
	def isMatch(s: Checkable): Boolean = (this.language.isEmpty || s.language == this.language.get) && checker.isMatch(s)
}

trait RuleSystem extends Rule {
	def combineRules: RuleSystem
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem
	def stats: Iterable[String]
	def rules: Iterable[Rule]
	def expiring: IndexedSeq[DatabaseRuleInfo]
}

class FlatRuleSystem(initialRules: Iterable[DatabaseRule]) extends RuleSystem {
	val rules = initialRules.toSet
	val ruleStream = rules.toStream
	val expiring = rules.filter(r => r.expires.isDefined).toIndexedSeq.sortBy((r: DatabaseRuleInfo) => r.expires.get.getTime)
	def isMatch(s: Checkable): Boolean = rules.exists {
		_.isMatch(s)
	}
	def allMatches(s: Checkable) = ruleStream.flatMap((x: DatabaseRule) => x.allMatches(s))
	def combineRules: CombinedRuleSystem = new CombinedRuleSystem(initialRules)
	def copy(rules: Iterable[DatabaseRule]): RuleSystem = new FlatRuleSystem(rules)
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Set[Int]): RuleSystem = {
		val remaining = rules.filterNot(rule => deletedIds.contains(rule.dbId)).seq
		if (added.isEmpty && remaining.size == rules.size) this else copy(remaining ++ added)
	}
	protected def statsSummary(c: Checker): String = c.description(long = false)
	protected def statsPerCheckerType(rs: Iterable[Checker]): String = {
		val groups = rs.groupBy(c => statsSummary(c)).toIndexedSeq.sortBy((pair: (String, Iterable[Checker])) => pair._1)
		groups.map((pair: (String, Iterable[Checker])) => pair._1 + (if (pair._2.size > 1) {
			"=" + pair._2.size
		} else {
			""
		})).mkString(", ")
	}
	protected def statsPerRuleType(rs: Iterable[DatabaseRule]): String = statsPerCheckerType(rs.map(rule => rule.checker))
	def ruleStats: Iterable[String] = {
		val cased = rules.seq.groupBy(_.caseType)
		Seq("Case sensitive: " + statsPerRuleType(cased.getOrElse(CaseSensitive, None)),
			"Case insensitive: " + statsPerRuleType(cased.getOrElse(CaseInsensitive, None)))
	}
	def stats: Iterable[String] = {
		Seq((Seq("FlatRuleSystem", "with", "total", rules.size, "rules").mkString(" "))) ++ ruleStats
	}
}

class CombinedRuleSystem(initialRules: Iterable[DatabaseRule]) extends FlatRuleSystem(initialRules) {
	private val logger = NiceLogger("RuleSystem")
	val checkers: Map[Option[String], Iterable[Checker]] = extractCheckers.withDefaultValue(Set.empty)

	def extractCheckers: Map[Option[String], Iterable[Checker]] = {
		type func = Iterable[DatabaseRule] => Checker
		// using those functions as keys in the map "sets"
		val exactCs: func = texts => new SetExactChecker(CaseSensitive, texts.map(rule => rule.text))
		val exactCi: func = texts => new SetExactChecker(CaseInsensitive, texts.map(rule => rule.text))
		val cs: func = texts => new RegexChecker(CaseSensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
		val ci: func = texts => new RegexChecker(CaseInsensitive, texts.map(rule => rule.checker.regexPattern).mkString("|"))
		val groupedByLang = initialRules.groupBy(rule => rule.language)
		def splitCase(rs: Iterable[DatabaseRule]) = {
			rs.groupBy(rule => rule.checker match {
				case c: ExactChecker => if (rule.caseType == CaseSensitive) exactCs else exactCi
				case c: Checker => if (rule.caseType == CaseSensitive) cs else ci
			}).toSeq.map(pair => pair._1(pair._2))
		}
		val result = for ((lang, rs) <- groupedByLang) yield (lang, splitCase(rs))
		result.toMap
	}
	override def copy(rules: Iterable[DatabaseRule]) = new CombinedRuleSystem(rules)
	override def isMatch(s: Checkable): Boolean = {
		val selected = checkers(None) ++ checkers(Some(s.language))
		val result = selected.exists (x => x.isMatch(s))
		logger.debug(s"isMatch result for $s: $result")
		result
	}
	override def allMatches(s: Checkable): Iterable[DatabaseRuleInfo] = {
		if (isMatch(s)) ruleStream flatMap (x => x.allMatches(s)) else Seq.empty[DatabaseRuleInfo]
	}
	override def combineRules: CombinedRuleSystem = this
	override def ruleStats: Iterable[String] = {
		val types = new mutable.HashMap[String, Set[Checker]]().withDefaultValue(Set.empty[Checker])
		for ((lang, c) <- checkers) {
			for (checker <- c) {
				val text = (if (checker.caseType == CaseSensitive) "Case sensitive" else "Case insensitive") + " (" + lang.getOrElse("All langugages") + ") : "
				types(text) = types(text) + checker
			}
		}
		types.map(pair => {
			val (text, checkers) = pair
			text + statsPerCheckerType(checkers)
		}).toSeq.sorted
	}
	override def statsSummary(c: Checker): String = c.description(long = true)
	override def stats: Iterable[String] = {
		val checkerCount = checkers.values.map(t => t.size).sum
		Seq((Seq("CombinedRuleSystem", "with", "total", rules.size, "rules", "and", checkerCount, "checkers").mkString(" "))) ++ ruleStats
	}
}





