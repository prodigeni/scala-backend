package com.wikia.phalanx

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, RichHttp, Request, Status, Version, Response, Message}
import com.twitter.finagle.util.TimerFromNettyTimer
import com.twitter.util.{Time, TimerTask, FuturePool, Future}
import com.wikia.wikifactory._
import java.util.regex.PatternSyntaxException
import java.util.{Calendar, Date}
import util.parsing.json.{JSONArray, JSONFormat}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.util.HashedWheelTimer
import org.slf4j.{LoggerFactory, Logger}
import java.io.{FileInputStream, File}
import sys.SystemProperties
import scala.collection.JavaConversions._

class ExceptionLogger[Req,Rep](val logger: Logger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(LoggerFactory.getLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure( (exception) => {
			logger.error("Exception in service" , exception)
		})
	}
}

object Respond {
	def apply(content: String, status: HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
		val response = Response(Version.Http11, status)
		response.contentType = contentType
		response.contentString = content
		response
	}
	def json(data: Traversable[Int]) = Respond(JSONArray(data.toList).toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info + "\n", status)
	def ok(s: String = "") = Respond("ok\n" + s)
	def failure(s: String = "") = Respond("failure\n" + s)
}


class MainService(var rules: Map[String, RuleSystem], val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem],
                  val scribe:Service[Map[String, Any], Unit]) extends Service[Request, Response] {
	//def this(rules: Map[String, RuleSystem]) = this(rules, (x, y) => x)
	val logger = LoggerFactory.getLogger(classOf[MainService])
	val threaded = FuturePool.defaultPool
	var nextExpireDate: Option[Date] = None
	var expireWatchTask: Option[TimerTask] = None
	val timer = new TimerFromNettyTimer(new HashedWheelTimer())
	watchExpired()

	override def release() {
		logger.info("Stopping expired timer")
		timer.stop()
		logger.info("Stopping scribe client")
		scribe.release()
	}

	def watchExpired() {
		val minDates = rules.values.flatMap(ruleSystem => ruleSystem.expiring.headOption.map(rule => rule.expires.get))
		expireWatchTask.map(task => {
			logger.info("Old expire task " + task + "cancelled")
			task.cancel()
		})
		if (minDates.isEmpty) {
			nextExpireDate = None
			expireWatchTask = None
			logger.info("Expire task not required")
		} else {
			val c = java.util.Calendar.getInstance(DB.dbTimeZone)
			c.setTime(minDates.min)
			c.set(Calendar.SECOND, 0)
			c.add(Calendar.MINUTE, 1)
			nextExpireDate = Some(c.getTime)
			expireWatchTask = Some(timer.schedule(Time(nextExpireDate.get)) {
				expire()
			})
			logger.info("Scheduling expire task at " + nextExpireDate.get)
		}
	}
	def expire() {
		// todo: somehow run this on main finagle thread?
		val now = new Date().getTime
		val expired = rules.values.flatMap(ruleSystem => ruleSystem.expiring.takeWhile(rule => rule.expires.get.getTime <= now)).map(r => r.dbId)
		afterReload(expired)
	}
	def afterReload(expired: Traversable[Int]): Future[Unit] = {
		threaded({
			reloader(rules, expired)
		}) map (newRules => {
			rules = newRules
			watchExpired()
		})
	}
	def selectRuleSystem(request: Request): Option[RuleSystem] = {
		request.params.get("type") match {
			case Some(s: String) => rules.get(s)
			case None => None
		}
	}
	def handleCheckOrMatch(func: (RuleSystem, Checkable) => Response)(request: Request): Future[Response] = selectRuleSystem(request) match {
		case None => Future(Respond.error("Unknown type parameter"))
		case Some(ruleSystem: RuleSystem) => threaded {
			request.params.get("content") match {
				case None => Respond.error("content parameter is missing")
				case Some(s: String) => ({
					func(ruleSystem, Checkable(s, request.params.getOrElse("lang", "en")))
				})
			}
		}
	}
	val handleCheck = handleCheckOrMatch((ruleSystem, c) => {
		if (ruleSystem.isMatch(c)) Respond.failure() else Respond.ok()
	}) _
	val handleMatch = handleCheckOrMatch((ruleSystem, c) => {
		val matches = ruleSystem.allMatches(c).map(r => r.dbId)
		Respond.json(matches)
	}) _
	def validateRegex(request: Request) = {
		val s = request.params.getOrElse("regex", "")
		try {
			s.r
			Respond.ok()
		} catch {
			case e: PatternSyntaxException => Respond.failure()
		}
	}
	def stats = {
		val response = (rules.toSeq.map(t => {
			val (s, ruleSystem) = t
			s + ":\n" + (ruleSystem.stats.map {
				"  " + _
			}.mkString("\n")) + "\n"
		}) ++ Seq("Next rule expire date: " + nextExpireDate)).mkString("\n")
		response
	}
	def apply(request: Request): Future[Response] = {
		Path(request.path) match {
			case Root => Future(Respond("PHALANX ALIVE"))
			case Root / "check" => handleCheck(request)
			case Root / "match" => handleMatch(request)
			case Root / "validate" => threaded {
				validateRegex(request)
			}
			case Root / "reload" => {
				val ids = for (x <- request.getParam("changed", "").split(',')) yield x.toInt
				afterReload(ids).map(_ => Respond.ok())
			}
			case Root / "stats" => threaded {
				Respond(stats)
			}
			case _ => Future(Respond.error("not found", Status.NotFound))
		}
	}
}

object Main extends App {
	def loadProperties(fileName: String): Option[java.util.Properties] = {
		val file = new File(fileName)
		if (file.exists() && file.canRead) {
			val properties = new java.util.Properties()
			properties.load(new FileInputStream(file))
			Some(properties)
		} else None
	}
	// load config from first file that exists: phalanx.properties, phalanx.default.properties
	val loadedProperties = Stream("phalanx.properties", "phalanx.default.properties").map(loadProperties).flatten.head.toMap
	sys.props ++= loadedProperties

	val logger = LoggerFactory.getLogger("Main")
	logger.info("Properties loaded")
	val scribe = {
		val scribetype =sys.props("com.wikia.phalanx.Scribe")
		logger.info("Creating scribe client ("+scribetype+")")
		scribetype match {
			case "send" => new ScribeClient("localhost", 1463)
			case "buffer" => new ScribeBuffer()
			case "discard" => new ScribeDiscard()
		}
	}
	scribe(("test", Map(("somekey", "somevalue"))))() // make sure we're connected
	val port = sys.props("com.wikia.phalanx.port").toInt

	logger.info("Loading rules from database")
	val db = new DB(DB.DB_MASTER, "", "wikicities").connect()
	val mainService = new MainService(RuleSystem.fromDatabase(db), (old, changed) => RuleSystem.reloadSome(db, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe.category("log_phalanx"))

	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen mainService)
	logger.info("Server started on port: " + port)
	logger.trace("Initial stats: \n" + mainService.stats)
}

