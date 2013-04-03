package com.wikia.utils

class ConfigException(msg:String) extends Exception(msg) {
}

class SysPropConfig {
  outer =>
  protected type FValue[+T] = () => T
  protected def get(key: String):Option[String] = sys.props.get(key)
  protected case class Remembered(group: String, key:String, doc:String, default:String, getter:FValue[Any]) {
  }
  protected def remember[T](group: String, key:String, doc:String, default:String, getter:FValue[T]):FValue[T] = {
    assert(!remembered.contains(key))
    remembered(key) = Remembered(group, key, doc, default, getter)
    getter
  }
  protected val remembered = collection.mutable.HashMap.empty[String, Remembered]

  def bool(key:String, doc: String, default:Boolean, group:String):FValue[Boolean] = {
    remember(group, key, doc, default.toString,
      () => get(key) match {
        case Some("true") => true
        case Some("yes") => true
        case Some("on") => true
        case Some("1") => true
        case Some("false") => false
        case Some("no") => false
        case Some("off") => false
        case Some("0") => false
        case Some("") => default
        case Some(x) => throw new ConfigException(s"Invalid value '$x' for boolean configuration key '$key'")
        case None => default
      })
  }
  def string(key:String, doc: String, default:String, group:String):FValue[String] = {
    remember(group, key, doc, default.toString,
      () => get(key) match {
        case Some("") => default
        case Some(x) => x
        case None => default
      })
  }
  def int(key:String, doc: String, default:Int, group:String):FValue[Int] = {
    remember(group, key, doc, default.toString,
      () => get(key) match {
        case Some("") => default
        case Some(x) => try {
          x.toInt
        } catch {
          case _:NumberFormatException => throw new ConfigException(s"Invalid value '$x' for integer configuration key '$key'")
        }
        case None => default
      })
  }
  case class Group(groupDoc:String) {
    def bool(key:String, doc: String, default:Boolean):FValue[Boolean] = outer.bool(key, doc, default, groupDoc)
    def string(key:String, doc: String, default:String):FValue[String] = outer.string(key, doc, default, groupDoc)
    def int(key:String, doc: String, default:Int):FValue[Int] = outer.int(key, doc, default, groupDoc)
  }
  def setDefaults() {
    remembered.foreach( (t:(String, Remembered)) => t match {
      case (key, r) => if (!sys.props.contains(key) && r.default.nonEmpty) sys.props(key) = r.default
    })
  }
  def defaultConfigContents: String = {
    val groups = remembered.values.groupBy(_.group).toSeq.sortBy(_._1)
    groups.map( {
      case (groupDoc, props) => (Seq(if (groupDoc.nonEmpty) s"# $groupDoc\n" else "") ++ props.toSeq.sortBy(_.key).map(r =>
        (if (r.doc.nonEmpty) s"\n# ${r.doc}\n" else "")+ s"# ${r.key}=${r.default}\n")).mkString
    }).mkString("\n")
  }
  def markdownHelp(title: String = "Property configuration"):String = {
    val groups = remembered.values.groupBy(_.group).toSeq.sortBy(_._1)
    val content = groups.map( {
      case (groupDoc, props) => (Seq(s"## $groupDoc\n") ++ props.toSeq.sortBy(_.key).map(r =>
        s"* ${r.key}=${r.default}\n" + (if (r.doc.nonEmpty) s"${r.doc}\n" else ""))).mkString
    }).mkString("\n")
    s"# $title\n$content"
  }
}