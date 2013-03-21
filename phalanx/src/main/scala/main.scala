package com.wikia.phalanx
import collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.{Http, Request, Status, Version, Response, Message}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util._
import com.wikia.wikifactory._
import java.io.{FileInputStream, File}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import util.parsing.json.{JSONObject, JSONArray, JSONFormat}
import com.twitter.finagle.http.RichHttp

class ExceptionLogger[Req, Rep](val logger: NiceLogger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(NiceLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure((exception) => {
      exception match {
        case e: java.util.concurrent.CancellationException => ()
        case _ => logger.exception("Exception in service", exception)
      }
		})
	}
}

object Respond {
	def apply(content: String, status: org.jboss.netty.handler.codec.http.HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
		val response = Response(Version.Http11, status)
		response.contentType = contentType
		response.contentString = content
		response
	}
	def json(data: Iterable[DatabaseRuleInfo]) = {
		val jsonData = JSONArray(data.toList.map(x => x.toJSONObject))
		Respond(jsonData.toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def json(data: Map[String, DatabaseRuleInfo]) = {
		val jsonData = JSONObject(data.mapValues(x => x.toJSONObject))
		Respond(jsonData.toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info + "\n", status)
	val ok = Respond("ok\n")
	val failure = Respond("failure\n")
	val contentMissing = error("content parameter is missing", Status.BadRequest)
	val unknownType = error("Unknown type parameter", Status.BadRequest)
  val internalError = error("Internal server error")
  val notFound = error("not found", Status.NotFound)
}


object Main extends App {
	def loadProperties(fileName: String): java.util.Properties = {
		val file = new File(fileName)
		val properties = new java.util.Properties()
		properties.load(new FileInputStream(file))
		println("Loaded properties from " + fileName)
		properties
	}
	def wikiaProp(key: String) = sys.props("com.wikia.phalanx." + key)
  def wikiaPropOption(key: String) = sys.props.get("com.wikia.phalanx." + key)
	def scribeClient() = {
		val host = wikiaProp("scribe.host")
		val port = wikiaProp("scribe.port").toInt
		new ScribeClient("log_phalanx", host, port)
	}

	val cfName: Option[String] = sys.props.get("phalanx.config") orElse {
		// load config from first config file that exists
		Seq(
			"phalanx.properties",
			"/usr/wikia/conf/current/phalanx.properties",
			"/usr/wikia/docroot/phalanx.properties",
			"phalanx.default.properties",
			"/usr/wikia/phalanx/phalanx.default.properties")
			.find(fileName => {
			val file = new File(fileName)
			file.exists() && file.canRead
		})
	}
	cfName match {
		case Some(fileName) => sys.props ++= loadProperties(fileName).toMap
		case None => {
			println("Don't know where to load configuration from.")
			System.exit(2)
		}
	}
	val logger = NiceLogger("Main")
	val versionString = s"Phalanx server version ${PhalanxVersion.version}"
	logger.info(s"$versionString starting, properties loaded from ${cfName.get}")
	val preloaded = PackagePreloader(this.getClass, Seq(
		"com.wikia.phalanx",
		"com.twitter.finagle.http",
		"com.twitter.util"
	))
	logger.info("Preloaded " + preloaded.size + " classes")
	val scribe = {
		val scribetype = wikiaProp("scribe")
		logger.info("Creating scribe client (" + scribetype + ")")
		scribetype match {
			case "send" => scribeClient()
			case "buffer" => new ScribeBuffer(scribeClient(), wikiaProp("scribe.flushperiod").toInt.milliseconds)
			case "discard" => new ScribeDiscard()
		}
	}
	val port = wikiaProp("port").toInt
	val threadCount: Option[Int] = wikiaProp("threads") match {
		case s: String if (s != null && s.nonEmpty) => Some(s.toInt)
		case _ => None
	}
  val notifyNodes = wikiaProp("notifynodes") match {
    case s: String if (s != null && s.nonEmpty) => s.split(' ').toSeq
    case _ => Seq.empty
  }
  val database = new DB(DB.DB_MASTER, None, "wikicities")
  logger.info("Connecting to database from configuration file " + database.config.sourcePath)
	val dbSession = database.connect()
  logger.info("Loading rules from database")
  val loader:RuleSystemLoader = CombinedLoader
	val mainService = new MainService(
		(old, changed) => loader.reloadSome(dbSession, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe,
		threadCount,
    notifyNodes
	)
	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
    .maxConcurrentRequests(600)
		.sendBufferSize(64*1024)
		.recvBufferSize(512*1024)
    .cancelOnHangup(true)
		.backlog(1000)
    .keepAlive(true)
    .hostConnectionMaxIdleTime(3.seconds)
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen NewRelic andThen mainService)
	logger.info(s"Listening on port: $port")
	logger.trace("Initial stats: \n" + mainService.statsString)

	sys.addShutdownHook {
		logger.warn("Terminating")
		server.close(3.seconds)
		mainService.close()
		logger.warn("Shutdown complete")
	}
}

