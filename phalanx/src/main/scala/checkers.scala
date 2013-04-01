package com.wikia.phalanx

class InvalidRegex(regex: String, inner: Throwable) extends Exception(regex, inner) {

}

abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	def description(long: Boolean = false): String
  def caseType: CaseType
  override def toString:String = description(false)
}

case class ExactChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s) == text
	def description(long: Boolean = false): String = "exact phrase"
}

case class ContainsChecker(caseType: CaseType, text: String) extends Checker {
  private final val regexSpecials = "\\[].*+{}^$|?()"
	def isMatch(s: Checkable): Boolean = caseType(s).contains(text)
	def regexPattern = text.map(c => if (regexSpecials.contains(c)) "\\"+c else c).mkString
	def description(long: Boolean = false): String = "contains"
}


case class Re2RegexChecker(caseType: CaseType, text: String) extends Checker {
  import com.logentries.re2.{RE2, Options}
  def baseOptions = new Options().setMaxMem(128*1024*1024).setNeverCapture(true).setLogErrors(false)
  private final val flags = Map(
   CaseSensitive -> baseOptions.setCaseSensitive(true),
   CaseInsensitive -> baseOptions.setCaseSensitive(false)
  )
  val regex = try {
    new RE2(text, flags(caseType))
  } catch {
    case e: Throwable => throw new InvalidRegex(text, e)
  }
	def isMatch(s: Checkable): Boolean = regex.partialMatch(s.text)
	def regexPattern = "("+text+")"
  override def finalize() {
    regex.close() // supposedly required to release memory allocated in native library
  }
	def description(long: Boolean = false): String = if (long) "re2-regex (" + text.length + " characters)" else "re2-regex"
}

case class SetExactChecker(caseType: CaseType, origTexts: Iterable[String]) extends Checker {
	val texts = if (caseType == CaseSensitive) origTexts.toSet else origTexts.map(s => s.toLowerCase).toSet
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
	override def toString = "SetExactChecker(" + texts.size + ")"
	def description(long: Boolean = false): String = "exact set of " + texts.size + " phrases"
}

case class JavaRegexChecker(caseType: CaseType, text: String) extends Checker {
  import java.util.regex.Pattern
  final val CSPatternOptions = Pattern.UNICODE_CASE | Pattern.DOTALL
  final val CIPatternOptions = Pattern.UNICODE_CASE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE
  val regex = try {
    InvalidRegex.logger.warn(s"This java regex should not be created: $text")
    Pattern.compile(text, if (caseType == CaseSensitive) CSPatternOptions else CIPatternOptions)
  } catch {
    case e: Throwable => throw new InvalidRegex(text, e)
  }
  def isMatch(s: Checkable): Boolean = regex.matcher(s.text).find()
  def regexPattern = "("+text+")"
  def description(long: Boolean = false): String = if (long) "java-regex (" + text.length + " characters)" else "java-regex"
}


object InvalidRegex {
  val logger = NiceLogger("InvalidRegex")
  def checkForError(regex: String):Option[String] = try {
    regex.r
    if (regex.contains("(?!")) Some("Invalid perl operator (?!") else None
  } catch {
    case e: java.util.regex.PatternSyntaxException => Some(e.getMessage)
    case e: Throwable => {
      logger.exception(s"Unexpected error parsing regex '$regex'", e)
      Some("Internal server error")
    }
  }
}

object Checker {
  final val logger = NiceLogger("Checker")
  final val TYPE_USER = 8
  var groupCount = Configuration.workerGroups() match {
    case 0 => Main.processors
    case x => x
  }
  def splitIntoGroups(whole: Iterable[String]):Iterable[Iterable[String]] = {
    if (groupCount==1) Seq(whole) else {
      val all = whole.toSeq
      val groupSize = Seq(all.size / groupCount, 1).max
      all.grouped(groupSize).toIterable
    }
  }
  def regex(caseType: CaseType, text: String, longContent:Boolean):Checker = {
    InvalidRegex.checkForError(text) match {
      case Some(s) => throw new InvalidRegex(s, null)
      case _ => ()
    }
    //if (longContent) Re2RegexChecker(caseType,text) else JavaRegexChecker(caseType, text)
    Re2RegexChecker(caseType,text)
  }
  def regex(caseType: CaseType, alternatives: Iterable[String]):Checker = regex(caseType, alternatives.mkString("|"), true)
  def regex(caseType: CaseType, text: String, typeMask:Int):Checker = regex(caseType, text, (typeMask & TYPE_USER) != 0)
  def contains(caseType: CaseType, text: String):Checker = ContainsChecker(caseType, text)
  def exact(caseType: CaseType, text: String):Checker = ExactChecker(caseType, text)
  def combine(checkers: Iterable[Checker]):Iterable[Checker] = {
    val regexPattern:PartialFunction[Checker, String] = (checker:Checker) => checker match {
      case cc:ContainsChecker => cc.regexPattern
      case cc:Re2RegexChecker => cc.regexPattern
      case cc:JavaRegexChecker if (InvalidRegex.checkForError(cc.text) == None) => cc.regexPattern
    }
    val exactPattern:PartialFunction[Checker, String] = (checker:Checker) => checker match {
      case ExactChecker(_, text) => text
    }
    // using those functions as keys in the map
    type func = Iterable[Checker] => Iterable[Checker]
    val exactCs: func = checkers => Seq(SetExactChecker(CaseSensitive, checkers.collect(exactPattern)))
    val exactCi: func = checkers => Seq(SetExactChecker(CaseInsensitive, checkers.collect(exactPattern)))
    val regexCS: func = checkers => splitIntoGroups(checkers.collect(regexPattern)).map(regex(CaseSensitive,_))
    val regexCI: func = checkers => splitIntoGroups(checkers.collect(regexPattern)).map(regex(CaseInsensitive,_))
    val result = checkers.groupBy(checker => checker match {
      case ExactChecker(CaseSensitive, _) => exactCs
      case ExactChecker(CaseInsensitive, _) => exactCi
      case c:Checker if c.caseType == CaseSensitive => regexCS
      case c:Checker if c.caseType == CaseInsensitive => regexCI
    }).toSeq.map(pair => pair._1(pair._2))
    result.flatten
  }
}
