package com.wikia.phalanx

import scala.collection.{GenSeq, mutable}
import com.twitter.util.Time
import util.parsing.json.JSONObject

class RuleViolation(rule: DatabaseRuleInfo) extends Exception {
	def ruleIds = Seq(rule.dbId)
}

case class Checkable(text: String, language: String = "en") {
	val lcText = text.toLowerCase
  override def toString:String = text
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
	def firstMatch(s: Checkable): Option[DatabaseRule]
	def check(s: Checkable) {
		val m = firstMatch(s)
		if (m.nonEmpty) throw new RuleViolation(m.get)
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
		if (regex) Checker.regex(caseType, text, typeMask)
		else {
			if (exact) Checker.exact(caseType, caseType(text)) else Checker.contains(caseType, caseType(text))
		}
	}
	def firstMatch(s: Checkable): Option[DatabaseRule] = if ((this.language.isEmpty || s.language == this.language.get) && checker.isMatch(s)) Some(this) else None
  override def toString = s"Rule($dbId, $text, $checker)"
	assert(language.isEmpty || language.get != "")

}

trait RuleSystem extends Rule {
	def combineRules: RuleSystem
	def reloadRules(added: Iterable[DatabaseRule], deletedIds: Iterable[Int]): RuleSystem
	def stats: Iterable[String]
	def rules: Iterable[DatabaseRule]
	def expiring: IndexedSeq[DatabaseRuleInfo]
  def checkerDescriptions: Iterable[String] = Seq.empty
}

class FlatRuleSystem(initialRules: Iterable[DatabaseRule]) extends RuleSystem {
  val logger = NiceLogger("RuleSystem")
	val rules = initialRules.toSet
	val ruleStream = initialRules.toIndexedSeq.sortBy(_.text.length).toStream
	val expiring = rules.filter(r => r.expires.isDefined).toIndexedSeq.sortBy((r: DatabaseRuleInfo) => r.expires.get)
	def firstMatch(s: Checkable) = ruleStream.flatMap((x: DatabaseRule) => x.firstMatch(s)).headOption
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
    groups.map((pair: (String, Iterable[Checker])) => {
      pair._1 + (if (pair._2.size > 1) {
        "=" + pair._2.size
      } else {
        ""
      })
    }).mkString(", ")
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
  type checkerSeq = GenSeq[MultiChecker]
  val autoParallel : (checkerSeq =>  checkerSeq) = if (Config.autoParallel()) s => s.par else s => s
	val checkers: Map[String, checkerSeq] = {
		val groupedByLang = initialRules.groupBy(rule => rule.language)
    val defaults = groupedByLang.get(None).map(_.toSeq).getOrElse(Seq.empty)
		val result = for ((lang, rs) <- groupedByLang if lang.nonEmpty) yield (lang.get, autoParallel(Checker.combine(defaults ++ rs).toSeq))
    val allLang = autoParallel(Checker.combine(defaults).toSeq)
		result.toMap.updated("", allLang).withDefaultValue(allLang) // for stats
	}
	override def copy(rules: Iterable[DatabaseRule]) = new CombinedRuleSystem(rules)
	override def firstMatch(s: Checkable): Option[DatabaseRule] = checkers(s.language).flatMap(c => c.firstMatch(s)).headOption
	override def combineRules: CombinedRuleSystem = this
	override def ruleStats: Iterable[String] = {
		val types = new mutable.HashMap[String, List[Checker]]().withDefaultValue(Nil)
		for ((lang, c) <- checkers) {
			for (checker <- c.seq) {
				val text = (if (checker.caseType == CaseSensitive) "Case sensitive" else "Case insensitive") + " (" + (if (lang.nonEmpty) lang else "All") + ") "
				types(text) = types(text).::(checker)
			}
		}
		types.map(pair => {
			val (text, checkers) = pair
      val c = checkers.toIndexedSeq
			text + s"[${c.size} checkers]: " + statsPerCheckerType(checkers)
		}).toSeq.sorted
	}
	override def statsSummary(c: Checker): String = c.description(long = true)
	override def stats: Iterable[String] = {
		val checkerCount = checkers.values.map(t => t.size).sum
		Seq((Seq("CombinedRuleSystem", "with", "total", rules.size, "rules", "and", checkerCount, "checkers").mkString(" "))) ++ ruleStats
	}
  override def checkerDescriptions = {
    val c = checkers("").seq.toSeq.sortBy(c => c.getClass.getCanonicalName)
    c.map(checker => checker match {
      case re2: Re2RegexMultiChecker => Seq(s"Re2 checker (${re2.rules.size} rules):") ++
        re2.rules.sortBy(r => r.text).map(r => s"    ${r.dbId}: ${r.text}")
      //case fstl: FSTLChecker => Seq(s"FSTL checker (${fstl.caseType} ${fstl.texts.size} rules,  ${fstl.matcher.size} nodes):") ++
      //  fstl.texts.values.toSeq.sortBy(r=> r.text).map(r => s"    ${r.dbId}: ${r.text}")
      case _ => Seq.empty
    }).flatten
  }
}





