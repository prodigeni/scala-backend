package com.wikia.phalanx

import com.twitter.finagle.http.{Http, RichHttp, Request, Status, Version, Response, Message}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.path._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.codehaus.jackson.map.ObjectMapper
import com.wikia.wikifactory._
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.logging.Logger

object Respond {
  val jsonMapper = new ObjectMapper()
  def apply(content: String, status: HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
    val response = Response(Version.Http11, status)
    response.contentType = contentType
    response.contentString = content
    Future(response)
  }
  def json(data: Any) = Respond(jsonMapper.writeValueAsString(data), Status.Ok, Message.ContentTypeJson)
  def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info+"\n", status)
}

class MainService(var rules: Map[String,RuleSystem]) extends Service[Request, Response] {
	val logger = Logger.get()
	logger.setLevel(Logger.DEBUG)
  def selectRuleSystem(request: Request):Option[RuleSystem] = {
    request.params.get("type") match {
      case Some(s:String) => rules.get(s)
      case None => None
    }
  }
	def check(request: Request) = 1
  def apply(request: Request) = {
    Path(request.path) match {
      case Root => Respond("PHALANX ALIVE")
      case Root / "check" => {
        selectRuleSystem(request) match {
          case None => Respond.error("Unknown type parameter")
          case Some(ruleSystem:RuleSystem) => {
            request.params.get("content") match {
              case Some(s:String) => {
	              val matched = ruleSystem.isMatch(s)
	              val response = if (matched) "failure" else "ok"
	              Respond(response)
              }
              case None =>  Respond.error("content parameter is missing")
            }
          }
        }
      }
      case Root / "match" => {
        selectRuleSystem(request) match {
          case None => Respond.error("Unknown type parameter")
          case Some(ruleSystem:RuleSystem) => {
            request.params.get("content") match {
              case Some(s:String) =>   {
                val matches = ruleSystem.allMatches(s).map( r => r.dbId)
                Respond.json(matches.toArray[Int])
              }
              case None =>  Respond.error("content parameter is missing")
            }
        }
      }
      }
      case Root / "reload" => {
        Respond("reloaded")
      }
      case Root / "stats" => {
        val response = rules.toSeq.map( t => { val (s, rs) = t; s + ": " + rs.rules.size.toString} ).mkString("\n")
        Respond(response)
      }
      case _ => Respond.error("not found", Status.NotFound)
    }
  }
}

object Main extends App {
	Logger.reset()
	Logger.get().setLevel(Logger.DEBUG) // make configurable
	val logger = Logger.get()
	logger.info("Loading rules from database")
	val db = new DB (DB.DB_MASTER, "", "wikicities").connect()
  val service = ExceptionFilter andThen new MainService(RuleSystem.fromDatabase(db))
  val port = Option(System getenv "PORT") match {
    case Some(p) => p.toInt
    case None => 8080
  }

  val config =  ServerBuilder()
    .codec(RichHttp[Request](Http()))
    .name("Phalanx")
    .bindTo(new java.net.InetSocketAddress(port))

  val server = config.build(service)


  logger.info("Server started on port: %s".format(port))
}

