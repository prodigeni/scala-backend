package com.wikia.phalanx

class InvalidRegex(regex: String, inner: Throwable) extends Exception(regex, inner)

trait Checker {
	def isMatch(s: Checkable): Boolean
	def description(long: Boolean = false): String
  def caseType: CaseType
  override def toString:String = description(false)
}

trait MultiChecker extends Checker {
  def firstMatch(s: Checkable): Option[DatabaseRule]
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

class Re2RegexMultiChecker(val caseType: CaseType, alternatives: Map[String, DatabaseRule]) extends MultiChecker {
  import com.logentries.re2.{RE2, Options}
  def baseOptions = new Options().setMaxMem(128*1024*1024).setNeverCapture(true).setLogErrors(true)
  private final val flags = Map(
    CaseSensitive -> baseOptions.setCaseSensitive(true),
    CaseInsensitive -> baseOptions.setCaseSensitive(false)
  )
  val rules = alternatives.values.toSeq
  val text = alternatives.keys.toSeq.sorted.mkString("|")
  val regex = try {
    new RE2(text, flags(caseType))
  } catch {
    case e: Throwable => throw new InvalidRegex(text, e)
  }
  def isMatch(s: Checkable): Boolean = regex.partialMatch(s.text)
  def firstMatch(s:Checkable): Option[DatabaseRule] = if (isMatch(s)) rules.find(r => r.checker.isMatch(s)) else None
  override def finalize() {
    regex.close() // supposedly required to release memory allocated in native library
  }
  def description(long: Boolean = false): String = if (long) "re2-regex (" + text.length + " characters)" else "re2-regex"
}


case class SetExactChecker(caseType: CaseType, origTexts: Map[String, DatabaseRule]) extends Checker with MultiChecker {
	val texts:Map[String, DatabaseRule] = if (caseType == CaseSensitive) origTexts else origTexts.map(t => t.copy(_1 = t._1.toLowerCase))
	def isMatch(s: Checkable): Boolean = texts.contains(caseType(s))
  def firstMatch(s: Checkable): Option[DatabaseRule] = texts.get(caseType(s))
	override def toString = "SetExactChecker(" + texts.size + ")"
	def description(long: Boolean = false): String = "exact set of " + texts.size + " phrases"
}

case class FSTLChecker(caseType: CaseType, origTexts: Map[String, DatabaseRule]) extends MultiChecker {
  import net.szumo.fstl.ac.StringMatcher
  val texts:Map[String, DatabaseRule] = if (caseType == CaseSensitive) origTexts else origTexts.map(t => t.copy(_1 = t._1.toLowerCase))
  val matcher = StringMatcher(texts.keys, net.szumo.fstl.CaseSensitive, texts.apply _) // we use our own case handling instead
  def isMatch(s: Checkable): Boolean = matcher.isMatch(caseType(s))
  def firstMatch(s: Checkable): Option[DatabaseRule] = {
    val matches = matcher(caseType(s))
    if (matches.hasNext) Some(matches.next()) else None
  }
  override def toString = "FSTLChecker(" + texts.size + ")"
  def description(long: Boolean = false): String = "contains set of " + texts.size + " phrases"
}

@deprecated("We use RE2 instead", "2013-04-16")
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
  import net.szumo.fstl.ac.StringMatcher
  val logger = NiceLogger("Checker")
  val TYPE_USER = 8
  val groupCount = Config.workerGroups() max 1
  val regexSpecialChars = Seq("\\", "[", "]", "$", "^", "(", ")", "|", ".", "+", "*", "?")
  val regexSpecialCharsAndSome = regexSpecialChars ++ Seq("/", "=", "#", "<", ">", "-") // people sometimes think they have to quote this
  val regexMeaninglessPrefixes = Seq(".*")
  val regexMeaninglessSuffixes = Seq(".*$", ".", ".*", ".+")
  val heurestics = Seq("https?://+[a-z0-9_./-]")
  val digitsAndDot = Set('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.')
  val regexSpecialMatcher = StringMatcher(regexSpecialChars, net.szumo.fstl.CaseSensitive)
  def unquoteRegex(pattern:String) = regexSpecialCharsAndSome.foldLeft(pattern)( (p:String, s:String) => p.replace("\\"+s,s))
  def removeQuoted(pattern:String) = regexSpecialCharsAndSome.foldLeft(pattern)( (p:String, s:String) => p.replace("\\"+s,""))

  def splitIntoGroups(whole: Iterable[(String, DatabaseRule)]):Iterable[Iterable[(String, DatabaseRule)]] = {
    if (groupCount<=1) Seq(whole) else whole.toSeq.sortBy(t => t._1).grouped(Seq(whole.size / groupCount, 1).max).toIndexedSeq
  }
  def regex(caseType: CaseType, text: String, longContent:Boolean):Checker = {
    // TODO: refactor this into composite optimizations
    InvalidRegex.checkForError(text) match {
      case Some(s) => throw new InvalidRegex(s, null)
      case _ => ()
    }
    var pattern = regexMeaninglessPrefixes.foldLeft(text)( (a:String,b:String) => a.stripPrefix(b))
    pattern = regexMeaninglessSuffixes.foldLeft(pattern)( (a:String,b:String) => a.stripSuffix(b))
    pattern = pattern.replace("\\n", "\n") // real \n // just for testing, ignores \\\\n
    var needsRealRegex = regexSpecialMatcher.isMatch(pattern)
    // try to optimize stuff that does not need to be a regex
    if (needsRealRegex && !regexSpecialMatcher.isMatch(removeQuoted(pattern))) {
      needsRealRegex = false
      pattern = unquoteRegex(pattern)
    }
    // guessing starts here
    if (needsRealRegex) {
      heurestics.find( p => pattern.startsWith(p)) match {
        case Some(start) => {
          needsRealRegex = false
          pattern = pattern.stripPrefix(start)
        }
        case None => if (pattern.toSet[Char].subsetOf(digitsAndDot)) needsRealRegex = false // assume they meant real dot
      }
    }
    InvalidRegex.checkForError(pattern) match {
      case Some(s) => pattern = text // we broke something, revert
      case _ => ()
    }
    if (needsRealRegex) {
      Re2RegexChecker(caseType, pattern)
    } else {
      ContainsChecker(caseType, pattern)
    }
  }
  def regex(caseType: CaseType, text: String, typeMask:Int):Checker =  regex(caseType, text, (typeMask & TYPE_USER) != 0)
  def contains(caseType: CaseType, text: String):Checker = ContainsChecker(caseType, text)
  def exact(caseType: CaseType, text: String):Checker = ExactChecker(caseType, text)
  def combine(rules: Iterable[DatabaseRule]):Iterable[MultiChecker] = {
    val regexPattern:PartialFunction[DatabaseRule, (String, DatabaseRule)] = (rule:DatabaseRule) => rule.checker match {
      case cc:Re2RegexChecker => (cc.regexPattern, rule)
      case cc:JavaRegexChecker if (InvalidRegex.checkForError(cc.text) == None) => (cc.regexPattern, rule)
    }
    val exactPattern:PartialFunction[DatabaseRule, (String, DatabaseRule)] = (rule:DatabaseRule) => rule.checker match {
      case ExactChecker(_, text) => (text, rule)
    }
    val containsPattern:PartialFunction[DatabaseRule, (String, DatabaseRule)] = (rule:DatabaseRule) => rule.checker match {
      case ContainsChecker(_, text) => (text, rule)
    }
    // using those functions as keys in the map
    type func = Iterable[DatabaseRule] => Iterable[MultiChecker]
    val exactCs: func = r => Seq(SetExactChecker(CaseSensitive, r.collect(exactPattern).toMap))
    val exactCi: func = r => Seq(SetExactChecker(CaseInsensitive, r.collect(exactPattern).toMap))
    val fstlCi:  func = r => Seq(FSTLChecker(CaseInsensitive, r.collect(containsPattern).toMap))
    val fstlCs:  func = r => Seq(FSTLChecker(CaseSensitive, r.collect(containsPattern).toMap))
    val regexCS: func = r => splitIntoGroups(r.collect(regexPattern)).map(r => new Re2RegexMultiChecker(CaseSensitive,r.toMap))
    val regexCI: func = r => splitIntoGroups(r.collect(regexPattern)).map(r => new Re2RegexMultiChecker(CaseInsensitive,r.toMap))
    val result = rules.groupBy(rule => rule.checker match {
      case ExactChecker(CaseSensitive, _) => exactCs
      case ExactChecker(CaseInsensitive, _) => exactCi
      case ContainsChecker(CaseSensitive, _) => fstlCs
      case ContainsChecker(CaseInsensitive, _) => fstlCi
      case c:Checker if c.caseType == CaseSensitive => regexCS
      case c:Checker if c.caseType == CaseInsensitive => regexCI
    }).toSeq.map(pair => pair._1(pair._2))
    result.flatten
  }
}
