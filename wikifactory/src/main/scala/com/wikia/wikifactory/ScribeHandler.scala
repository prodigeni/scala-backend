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
import scala.collection.immutable.HashMap
import scribe.thrift

object ScribeConf {
  val DEF_HOST : String = "127.0.0.1"
  val DEF_PORT : Int = 1463
  val DEF_TIME : Int = 10
}

class ScribeLogger ( hostname: String, port: Int, timeout: Int ) {
  var data: String = ""
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
  private def log() = {
    var entries = new java.util.ArrayList[LogEntry]()
    try {
      transport.open()
      entries.add( new LogEntry( cat, data ) )
      client.send_Log(entries)
      transport.close()
    } catch {
      case e: TException =>
        println("Thrift exception caught: " + e.getMessage + "\n\nStack:\n" + e.getStackTraceString)
      case e: Exception =>
        println("ScribeHandler exception caught: " + e.getMessage + "\n\nStack:\n" + e.getStackTraceString)
    }
  }

  def send( msg: Any ): Unit = msg match {
    case msg: JSONObject => { data = msg.toString(); log }
    case msg: HashMap[String, Any] => { data = JSONObject.apply( msg ).toString(); log }
    case _ => { data = msg.toString(); log }
  }
}
