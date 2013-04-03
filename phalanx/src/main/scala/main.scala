package com.wikia.phalanx

import collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.{Http, Request, Status, Version, Response, Message, RichHttp}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.{Future}
import com.wikia.utils.SysPropConfig
import com.wikia.wikifactory.DB
import java.io.{FileInputStream, File}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import util.parsing.json.{JSONObject, JSONArray, JSONFormat}


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

object Config extends SysPropConfig {
  private val cwp = "com.wikia.phalanx."
  private val sl = "org.slf4j.simpleLogger."

  val Main = Group("Main configuration")
  val port = Main.int(cwp+"port", "HTTP listening port", 4666)
  val notifyNodes = Main.string(cwp+"notifyNodes", "Space separated list of other nodes to notify", "")
  val userCacheMaxSize = Main.int(cwp+"userCacheMaxSize", "Size of LRU cache for user matching", (2 << 16)-1)
  val serviceThreadCount = Main.int(cwp+"serviceThreadCount", "Number of main service threads, or 0 for auto value", 0)
  val workerGroups = Main.int(cwp+"workerGroups", "Split each matching work into n parallel groups, or 0 for auto value", 0)
  val detailedStats = Main.bool(cwp+"detailedStats", "Keep detailed statistics", true)

  val Scribe = Group("Scribe configuration")
  val scribeType = Scribe.string(cwp+"scribe", "Scribe type: send, buffer or discard", "discard")
  val scribeHost = Scribe.string(cwp+"scribe.host", "Scribe host name", "localhost")
  val scribePort = Scribe.int(cwp+"scribe.port", "Scribe TCP port", 1463)
  val flushPeriod = Scribe.int(cwp+"scribe.flushperiod", "Scribe buffer flush period (milliseconds)", 1000)

  val Log = Group("Logging configuration")
  Log.string(sl+"defaultLogLevel", "Default logging level", "info")
  Log.string(sl+"log.Main", "", "")
  Log.string(sl+"log.MainService", "", "")
  Log.string(sl+"log.NewRelic", "", "")
  Log.string(sl+"log.RuleSystem", "", "")
  Log.string(sl+"log.RuleSystemLoader", "", "")
  Log.string(sl+"log.Scribe", "", "")
}



object Main extends App {
  if (args.contains("--print-defaults")) {
    println(Config.defaultConfigContents)
    sys.exit(0)
  }
  if (args.contains("--print-config-help")) {
    println(Config.markdownHelp("Phalanx properties help"))
    sys.exit(0)
  }

  final lazy val processors = Runtime.getRuntime.availableProcessors()
	def loadProperties(fileName: String): java.util.Properties = {
		val file = new File(fileName)
		val properties = new java.util.Properties()
		properties.load(new FileInputStream(file))
		println("Loaded properties from " + fileName)
		properties
	}
	def scribeClient() = {
		new ScribeClient("log_phalanx", Config.scribeHost(), Config.scribePort())
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
		val scribetype = Config.scribeType()
		logger.info("Creating scribe client (" + scribetype + ")")
		scribetype match {
			case "send" => scribeClient()
			case "buffer" => new ScribeBuffer(scribeClient(), Config.flushPeriod().milliseconds)
			case "discard" => new ScribeDiscard()
		}
	}
	val port = Config.port()
  val notifyNodes = Config.notifyNodes() match {
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

