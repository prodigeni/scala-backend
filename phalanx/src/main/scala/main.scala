package com.wikia.phalanx

import com.twitter.finagle.http.{Http, RichHttp, Request, Status, Version, Response, Message}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.path._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.codehaus.jackson.map.ObjectMapper

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
  def selectRuleSystem(request: Request):Option[RuleSystem] = {
    request.params.get("type") match {
      case Some(s:String) => rules.get(s)
      case None => None
    }
  }
  def apply(request: Request) = {
    Path(request.path) match {
      case Root => Respond("PHALANX ALIVE")
      case Root / "check" => {
        selectRuleSystem(request) match {
          case None => Respond.error("Unknown type parameter")
          case Some(ruleSystem:RuleSystem) => {
            request.params.get("content") match {
              case Some(s:String) =>  Respond( if (ruleSystem.isMatch(s)) "failure" else "ok" )
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
        Respond("stats")
      }
      case _ => Respond.error("not found", Status.NotFound)
    }
  }
}

object Main extends App {
  val ruleSystem = new RuleSystem(List(
    Rule.contains("fuck"), Rule.exact("Forbidden")
  ))

  val service = new MainService(Map( ("title", ruleSystem)) )
  val port = Option(System getenv "PORT") match {
    case Some(p) => p.toInt
    case None => 8080
  }

  val config =  ServerBuilder()
    .codec(RichHttp[Request](Http()))
    .name("Phalanx")
    .bindTo(new java.net.InetSocketAddress(port))
  val server = config.build(service)

  println("Server started on port: %s".format(port))
}

