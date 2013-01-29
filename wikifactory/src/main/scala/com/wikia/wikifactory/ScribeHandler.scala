package com.wikia.wikifactory

/**
 * Created with IntelliJ IDEA.
 * User: moli
**/

import org.apache.thrift.TException
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.{TFramedTransport, TSocket, TTransport}
import scribe.thrift.LogEntry
import util.parsing.json.JSONObject
import java.util
import scala.collection.JavaConversions._
import scribe.thrift.scribe.Client
import scribe.thrift

object ScribeConf {
  val DEF_HOST : String = "127.0.0.1"
  val DEF_PORT : Int = 1463
  val DEF_TIME : Int = 10
}

class ScribeLogger ( hostname: String, port: Int, timeout: Int ) {
  var cat: String = _

  val socket = new TSocket( hostname, port, timeout )
  val transport = new TFramedTransport(socket)
  val protocol = new TBinaryProtocol(transport, false, false)
  val client = new Client(protocol)

  def this() = this( ScribeConf.DEF_HOST, ScribeConf.DEF_PORT, ScribeConf.DEF_TIME)
  def this( hostname: String, port: Int ) = this( hostname, port, ScribeConf.DEF_TIME )
  def this( hostname: String ) = this( hostname, ScribeConf.DEF_PORT, ScribeConf.DEF_TIME )
  def this( port: Int ) = this( ScribeConf.DEF_HOST, port, ScribeConf.DEF_TIME )

  def category( id: String ) = { cat = id; this }
  def sendStrings(msg: Seq[String] ) {
    val entries = new java.util.ArrayList[LogEntry]()
    for (data <- msg) { entries.add(new LogEntry(cat, data)) }
    transport.open()
    try {
      client.send_Log(entries)
    } finally {
      transport.close()
    }
  }
  def sendJSONs(msg: Seq[JSONObject] ) { sendStrings(msg.map(_.toString())) }
  def sendMaps(msg: Seq[Map[String, Any]] ) { sendStrings(msg.map(JSONObject(_).toString())) }
}
