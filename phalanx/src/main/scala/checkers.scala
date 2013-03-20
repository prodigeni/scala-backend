package com.wikia.phalanx

class InvalidRegex(regex: String, inner: Throwable) extends Exception(regex, inner) {

}

abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	def description(long: Boolean = false): String
  def caseType: CaseType
  val text: String
}

case class ExactChecker(caseType: CaseType, text: String) extends Checker {
	def isMatch(s: Checkable): Boolean = caseType(s) == text
	override def toString = s"ExactChecker($caseType,$text)"
	def description(long: Boolean = false): String = "exact phrase"
}

case class ContainsChecker(caseType: CaseType, text: String) extends Checker {
  private final val regexSpecials = "\\[].*+{}^$|?()"
	def isMatch(s: Checkable): Boolean = caseType(s).contains(text)
	def regexPattern = text.map(c => if (regexSpecials.contains(c)) "\\"+c else c).mkString
	override def toString = s"ContainsChecker($caseType,$text)"
	def description(long: Boolean = false): String = "contains"
}


case class Re2RegexChecker(caseType: CaseType, text: String) extends Checker {
  import com.logentries.re2.{RE2, Options}
  def baseOptions = new Options().setNeverCapture(true).setMaxMem(64*1024*1024)
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
	override def toString = s"RegexChecker($caseType,$text)"
	def description(long: Boolean = false): String = if (long) "re2-regex (" + text.length + " characters)" else "re2-regex"
}

case class SetExactChecker(caseType: CaseType, origTexts: Iterable[String]) extends Checker {
  val text = ""
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
    Pattern.compile(text, if (caseType == CaseSensitive) CSPatternOptions else CIPatternOptions)
  } catch {
    case e: Throwable => throw new InvalidRegex(text, e)
  }
  def isMatch(s: Checkable): Boolean = regex.matcher(s.text).find()
  def description(long: Boolean = false): String = if (long) "java-regex (" + text.length + " characters)" else "java-regex"
  override def toString = s"JavaRegexChecker($text)"
}


object InvalidRegex {
  private val logger = NiceLogger("InvalidRegex")
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
  def regex(caseType: CaseType, text: String):Checker = {
    InvalidRegex.checkForError(text) match {
      case Some(s) => throw new InvalidRegex(s, null)
      case _ => ()
    }
    Re2RegexChecker(caseType,text)
  }
  def contains(caseType: CaseType, text: String):Checker = ContainsChecker(caseType, text)
  def exact(caseType: CaseType, text: String):Checker = ExactChecker(caseType, text)
  def combine(checkers: Iterable[Checker]):Iterable[Checker] = {
    type func = Iterable[Checker] => Iterable[Checker]
    // using those functions as keys in the map
    val exactCs: func = texts => Seq(SetExactChecker(CaseSensitive, texts.map(_.text)))
    val exactCi: func = texts => Seq(SetExactChecker(CaseInsensitive, texts.map(_.text)))
    val regexPattern:PartialFunction[Checker, String] = (checker:Checker) => checker match {
      case cc:ContainsChecker => cc.regexPattern
      case cc:Re2RegexChecker => cc.regexPattern
      case cc:JavaRegexChecker if (InvalidRegex.checkForError(cc.text) == None) => cc.text
    }
    val containsCs: func = texts => Seq(regex(CaseSensitive, texts.collect(regexPattern).mkString("|") ))
    val containsCi: func = texts => Seq(regex(CaseInsensitive, texts.collect(regexPattern).mkString("|") ))
    val regexCS: func = texts => Seq(regex(CaseSensitive, texts.collect(regexPattern).mkString("|") ))
    val regexCI: func = texts => Seq(regex(CaseInsensitive, texts.collect(regexPattern).mkString("|") ))
    val others: func = texts => texts
    checkers.groupBy(checker => checker match {
      case ExactChecker(CaseSensitive, _) => exactCs
      case ExactChecker(CaseInsensitive, _) => exactCi
      case ContainsChecker(CaseSensitive, _) =>  containsCs
      case ContainsChecker(CaseInsensitive, _) =>  containsCi
      case Re2RegexChecker(CaseSensitive, _) => regexCS
      case Re2RegexChecker(CaseInsensitive, _) => regexCI
      case JavaRegexChecker(CaseSensitive, _) => regexCS
      case JavaRegexChecker(CaseInsensitive, _) => regexCI
      case _ => others
    }).toSeq.map(pair => pair._1(pair._2)).flatten
  }
}
