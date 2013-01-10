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
import org.slf4j.LoggerFactory

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
	def ok(s:String = "") = Respond("ok\n"+s)
	def failure(s:String = "") = Respond("failure\n"+s)
}

class MainService(var rules: Map[String,RuleSystem], val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem]) extends Service[Request, Response] {
	def this(rules: Map[String, RuleSystem]) = this(rules, (x,y) => x)
	val logger = LoggerFactory.getLogger(classOf[MainService])
	val threaded = FuturePool.defaultPool
  def selectRuleSystem(request: Request):Option[RuleSystem] = {
    request.params.get("type") match {
      case Some(s:String) => rules.get(s)
      case None => None
    }
  }
	def handleCheckOrMatch(func: (RuleSystem, Checkable)=> Response)(request: Request):Future[Response] = selectRuleSystem(request) match {
		case None => Future(Respond.error("Unknown type parameter"))
		case Some(ruleSystem:RuleSystem) => threaded {
			request.params.get("content") match {
				case None => Respond.error("content parameter is missing")
				case Some(s:String) => ( { func(ruleSystem, Checkable(s, request.params.getOrElse("lang", "en"))) } )
			}
		}
	}
	val handleCheck = handleCheckOrMatch( (ruleSystem, c) => {
		if (ruleSystem.isMatch(c)) Respond.failure() else Respond.ok()
	}) _
	val handleMatch = handleCheckOrMatch( (ruleSystem, c) => {
		val matches = ruleSystem.allMatches(c).map( r => r.dbId)
		Respond.json(matches.toArray[Int])
	}) _
	def validateRegex(request: Request) = {
		val s = request.params.getOrElse("regex", "")
		try {
			s.r
			Respond.ok()
		} catch {
			case e:PatternSyntaxException => Respond.failure()
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
      case Root / "check" => handleCheck(request)
      case Root / "match" => handleMatch(request)
      case Root / "validate" => threaded { validateRegex(request) }
      case Root / "reload" => {
	      val ids = for (x<-request.getParam("changed", "").split(',')) yield x.toInt
         threaded( { reloader(rules,ids) }) map( newRules => {
		      rules = newRules
		      Respond.ok()
	      })
      }
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
	val mainService = new MainService(RuleSystem.fromDatabase(db), (old, changed) => RuleSystem.reloadSome(db, old, changed.toSet)) // todo: reload only changed rules
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

