package com.wikia.phalanx


abstract sealed class Checker {
	def isMatch(s: Checkable): Boolean
	val caseType: CaseType
	def description(long: Boolean = false): String
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


case class RegexChecker(caseType: CaseType, text: String) extends Checker {
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
	def regexPattern = text
  override def finalize() {
    regex.close() // supposedly required to release memory allocated in native library
  }
	override def toString = s"RegexChecker($caseType,$text)"
	def description(long: Boolean = false): String = if (long) "regex (" + text.length + " characters)" else "regex"
}

case class SetExactChecker(caseType: CaseType, origTexts: Iterable[String]) extends Checker {
	val texts = if (caseType == CaseSensitive) origTexts.toSet else origTexts.map(s => s.toLowerCase).toSet
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
	override def toString = "SetExactChecker(" + texts.size + ")"
	def description(long: Boolean = false): String = "exact set of " + texts.size + " phrases"
}

