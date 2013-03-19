package com.wikia.phalanx

import collection.mutable
import com.twitter.util.Time
import util.parsing.json.JSONObject
import collection.parallel.immutable.ParSeq

class RuleViolation(val rules: Iterable[DatabaseRuleInfo]) extends Exception {
	def ruleIds = rules.map(rule => rule.dbId)
}


case class Checkable(text: String, language: String = "en") {
	val lcText = text.toLowerCase
}

sealed trait CaseType {
	def apply(s: Checkable): String
	def apply(s: String): String
}

object CaseSensitive extends CaseType {
	def apply(s: Checkable) = s.text
	def apply(s: String) = s
	override def toString = "CS"
}

object CaseInsensitive extends CaseType {
	def apply(s: Checkable) = s.lcText
	def apply(s: String) = s.toLowerCase
	override def toString = "CI"
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
	val expires: Option[Time]
	val authorId: Int
	val typeMask: Int
	def toJSONObject:JSONObject = {
		JSONObject(Map(
			"id" → dbId,
			"text" → text,
			"type" → typeMask,
			"reason" → reason,
		  "caseSensitive" → caseSensitive,
		  "exact" → exact,
		  "regex" →  regex,
		  "language" → language.getOrElse(""),
		  "authorId" → authorId,
		  "expires" → (expires match {
			  case None => ""
				case Some(date) => com.wikia.wikifactory.DB.toWikiTime(date)
      }))
    )
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
	def exact(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Time] = None) = new DatabaseRule(text, 0, "", cs, true, false, lang, expires, 0, 0)
	def regex(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Time] = None) = new DatabaseRule(text, 0, "", cs, false, true, lang, expires, 0, 0)
	def contains(text: String, cs: Boolean = true, lang: Option[String] = None, expires: Option[Time] = None) = new DatabaseRule(text, 0, "", cs, false, false, lang, expires, 0, 0)
}

case class DatabaseRule(text: String, dbId: Int, reason: String, caseSensitive: Boolean, exact: Boolean, regex: Boolean,
                        language: Option[String], expires: Option[Time], authorId: Int, typeMask: Int) extends DatabaseRuleInfo with Rule {
	val caseType = if (caseSensitive || DatabaseRuleInfo.letterPattern.findFirstIn(text).isEmpty) CaseSensitive else CaseInsensitive
	val checker: Checker = {
		if (regex) Checker.regex(caseType, text)
		else {
			if (exact) Checker.exact(caseType, caseType(text)) else Checker.contains(caseType, caseType(text))
		}
	}
	def allMatches(s: Checkable): Iterable[DatabaseRuleInfo] = if (isMatch(s)) Some(this) else None
	def isMatch(s: Checkable): Boolean = (this.language.isEmpty || s.language == this.language.get) && checker.isMatch(s)
	assert(language.isEmpty || language.get != "")
}

trait RuleSystem extends Rule {
	def combineRules: RuleSystem
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Iterable[Int]): RuleSystem
	def stats: Iterable[String]
	def rules: Iterable[DatabaseRule]
	def expiring: IndexedSeq[DatabaseRuleInfo]
}

class FlatRuleSystem(initialRules: Iterable[DatabaseRule]) extends RuleSystem {
	val rules = initialRules.toSet
	val ruleStream = rules.toStream
	val expiring = rules.filter(r => r.expires.isDefined).toIndexedSeq.sortBy((r: DatabaseRuleInfo) => r.expires.get)
	def isMatch(s: Checkable): Boolean = rules.exists(_.isMatch(s))
	def allMatches(s: Checkable) = ruleStream.flatMap((x: DatabaseRule) => x.allMatches(s))
	def combineRules: CombinedRuleSystem = new CombinedRuleSystem(initialRules)
	def copy(rules: Iterable[DatabaseRule]): RuleSystem = new FlatRuleSystem(rules)
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Iterable[Int]): RuleSystem = {
		val changed = deletedIds.toSet ++ added.map(r => r.dbId)
		val remaining = rules.filterNot(rule => changed.contains(rule.dbId)).seq
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
  type checkerSeq = ParSeq[Checker]
	private val logger = NiceLogger("RuleSystem")

	val checkers: Map[Option[String], checkerSeq] = extractCheckers.withDefaultValue(ParSeq.empty)

	def extractCheckers: Map[Option[String], checkerSeq] = {
		val groupedByLang = initialRules.groupBy(rule => rule.language)
		val result = for ((lang, rs) <- groupedByLang) yield (lang, Checker.combine(rs.map(_.checker)).toIndexedSeq.par)
		result.toMap
	}
	override def copy(rules: Iterable[DatabaseRule]) = new CombinedRuleSystem(rules)
	override def isMatch(s: Checkable): Boolean = {
		val result = checkers(None).exists (_.isMatch(s)) || checkers(Some(s.language)).exists(_.isMatch(s))
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
	override def statsSummary(c: Checker): String = c.description(long = false)
	override def stats: Iterable[String] = {
		val checkerCount = checkers.values.map(t => t.size).sum
		Seq((Seq("CombinedRuleSystem", "with", "total", rules.size, "rules", "and", checkerCount, "checkers").mkString(" "))) ++ ruleStats
	}
}





