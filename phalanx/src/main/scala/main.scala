package com.wikia.phalanx

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, RichHttp, Request, Status, Version, Response, Message}
import com.twitter.util.{FuturePool, Future}
import com.wikia.wikifactory._
import java.util.regex.PatternSyntaxException
import org.codehaus.jackson.map.ObjectMapper
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.slf4j.{Logger, LoggerFactory}

object Respond {
  val jsonMapper = new ObjectMapper()
  def apply(content: String, status: HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
    val response = Response(Version.Http11, status)
    response.contentType = contentType
    response.contentString = content
	  response
  }
  def json(data: Any) = Respond(jsonMapper.writeValueAsString(data), Status.Ok, Message.ContentTypeJson)
  def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info+"\n", status)
	def ok = Respond("ok\n")
	def failure = Respond("failure\n")
}

class MainService(var rules: Map[String,RuleSystem], val reloader: Map[String, RuleSystem] => Map[String, RuleSystem]) extends Service[Request, Response] {
	def this(rules: Map[String, RuleSystem]) = this(rules, x => x)
	val logger = LoggerFactory.getLogger(classOf[MainService])
	val threaded = FuturePool.defaultPool
  def selectRuleSystem(request: Request):Option[RuleSystem] = {
    request.params.get("type") match {
      case Some(s:String) => rules.get(s)
      case None => None
    }
  }
	def handleCheckOrMatch(func: (RuleSystem, String)=> Response)(request: Request):Response = selectRuleSystem(request) match {
		case None => Respond.error("Unknown type parameter")
		case Some(ruleSystem:RuleSystem) => {
			request.params.get("content") match {
				case None => Respond.error("content parameter is missing")
				case Some(s:String) => ( { func(ruleSystem, s) } )
			}
		}
	}
	val handleCheck = handleCheckOrMatch( (ruleSystem, s) => {
		if (ruleSystem.isMatch(Checkable(s))) Respond.failure else Respond.ok
	}) _
	val handleMatch = handleCheckOrMatch( (ruleSystem, s) => {
		val matches = ruleSystem.allMatches(Checkable(s)).map( r => r.dbId)
		Respond.json(matches.toArray[Int])
	}) _
	def validateRegex(request: Request) = {
		val s = request.params.getOrElse("regex", "")
		try {
			s.r
			Respond.ok
		} catch {
			case e:PatternSyntaxException => Respond.failure
		}
	}
	def stats = {
		val response = rules.toSeq.map( t => {
			val (s, ruleSystem) = t
			s + ":\n" + (ruleSystem.stats.map { "  "+ _ }.mkString("\n")) +"\n"
		}).mkString("\n")
		response
	}
  def apply(request: Request):Future[Response] = {
    Path(request.path) match {
      case Root => Future(Respond("PHALANX ALIVE"))
      case Root / "check" => threaded { handleCheck(request) }
      case Root / "match" => threaded { handleMatch(request) }
      case Root / "validate" => threaded { validateRegex(request) }
      case Root / "reload" => threaded( { reloader(rules) } ) map( newRules => {
		      rules = newRules
		      Respond("reloaded")
	      })
      case Root / "stats" => threaded { Respond(stats) }
      case _ => Future(Respond.error("not found", Status.NotFound))
    }
  }
}

object Main extends App {
	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info") // todo: configurable instead?
	val logger = LoggerFactory.getLogger("Main")
	logger.info("Loading rules from database")
	val db = new DB (DB.DB_MASTER, "", "wikicities").connect()
	val mainService = new MainService(RuleSystem.fromDatabase(db), _ => RuleSystem.fromDatabase(db)) // todo: reload only changed rules
  val port = Option(System getenv "PORT") match {
    case Some(p) => p.toInt
    case None => 8080
  }

  val config =  ServerBuilder()
    .codec(RichHttp[Request](Http()))
    .name("Phalanx")
    .bindTo(new java.net.InetSocketAddress(port))

  val server = config.build(ExceptionFilter andThen mainService)
  logger.info("Server started on port: "+port)
	logger.info("Initial stats: \n"+mainService.stats)
}

