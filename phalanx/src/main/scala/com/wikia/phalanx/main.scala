package com.wikia.phalanx

import collection.JavaConversions._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.{Http, Request, Status, Version, Response, Message, RichHttp}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.Future
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
  def jsonFormatter(x : Any):String = x match {
    case s : String => "\"" + JSONFormat.quoteString(s) + "\""
    case None => "null"
    case Some(s) => jsonFormatter(s)
    case jo : JSONObject => jo.toString(jsonFormatter)
    case ja : JSONArray => ja.toString(jsonFormatter)
    case list : List[_] => JSONArray(list).toString(jsonFormatter)
    case map : Map[_,_] => JSONObject(map.map(x => (x._1.toString, x._2))).toString(jsonFormatter)
    case duration: com.twitter.util.Duration => (duration.inMicroseconds.toDouble / 1000000).toString
    case other => other.toString
  }

	def apply(content: String, status: org.jboss.netty.handler.codec.http.HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
		val response = Response(Version.Http11, status)
		response.contentType = contentType
		response.contentString = content
		response
	}
	def json(data: Iterable[DatabaseRuleInfo]) = {
		val jsonData = JSONArray(data.toList.map(x => x.toJSONObject))
		Respond(jsonData.toString(jsonFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def json(data: Map[String, DatabaseRuleInfo]) = {
		val jsonData = JSONObject(data.mapValues(x => x.toJSONObject))
		Respond(jsonData.toString(jsonFormatter), Status.Ok, Message.ContentTypeJson)
	}
  def json(data: StatsGatherer) = {
    val jsonData = JSONObject(data.toMap)
    Respond(jsonData.toString(jsonFormatter), Status.Ok, Message.ContentTypeJson)
  }
  def fromResource(path: String, contentType: String) = {
    val stream = getClass.getResourceAsStream(path)
    assert(stream != null, s"Could not find resource $path")
    val bytes = Array.ofDim[Byte](stream.available)
    stream.read(bytes)
    stream.close()
    val response = Response(com.twitter.finagle.http.Version.Http11, org.jboss.netty.handler.codec.http.HttpResponseStatus.OK)
    response.contentType = contentType
    response.content = org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer(bytes)
    response
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

  val Performance = Group("Performance tuning")
  val userCacheMaxSize = Performance.int(cwp+"userCacheMaxSize", "Size of LRU cache for user matching", (2 << 16)-1)
  val serviceThreadCount = Performance.int(cwp+"serviceThreadCount", "Number of main service threads, or 0 for auto value", 0)
  val workerGroups = Performance.int(cwp+"workerGroups", "Split each big regexp into n paraller smaller ones, or 1 for no split", 1)
  val autoParallel = Performance.bool(cwp+"autoParallel", "Use automatic parallel checking (one request processed in many threads)", false)
  val detailedStats = Performance.bool(cwp+"detailedStats", "Keep detailed statistics", true)
  val newRelic = Performance.bool(cwp+"newRelic", "Enable NewRelic (only if NewRelic agent is loaded and environment set)", true)
  val keepLastMinutes = Performance.int(cwp+"keepStats", "Keep separate performance stats for last n minutes (if detailedStats)", 5)
  val longRequestsMax = Performance.int(cwp+"longRequestsMax", "How many longest requests to remember", 10)
  val preloadClasses = Performance.bool(cwp+"preloadClasses", "Should all library classes be preloaded at server start", true)

  val Network = Group("Network configuration")
  val port = Network.int(cwp+"port", "HTTP listening port", 4666)
  val notifyNodes = Network.string(cwp+"notifyNodes", "Space separated list of other nodes to notify", "")
  val maxConcurrentRequests = Network.int(cwp+"maxConcurrentRequests", "Netty: maximum requests served at the same time", 100)
  val sendBufferSize = Network.int(cwp+"sendBufferSize", "Netty send buffer size", 128*1024)
  val recvBufferSize = Network.int(cwp+"recvBufferSize", "Netty reveive buffer size", 512*1024)
  val cancelOnHangup = Network.bool(cwp+"cancelOnHangup", "Cancel requests if connection lost", true)
  val backlog = Network.int(cwp+"backlog", "Listening socket backlog size", 1000)
  val keepAlive = Network.bool(cwp+"keepAlive", "Use HTTP 1.1 keep alives", true)
  val maxIdleTime = Network.float(cwp+"maxIdleTime", "How long to wait before closing a keep alive connection, in seconds", 1)

  val Scribe = Group("Scribe configuration - used to relay information about successful matches")
  val scribeType = Scribe.string(cwp+"scribe", "Scribe type: send, buffer or discard", "discard")
  val scribeHost = Scribe.string(cwp+"scribe.host", "Scribe host name", "localhost")
  val scribePort = Scribe.int(cwp+"scribe.port", "Scribe TCP port", 1463)
  val flushPeriod = Scribe.int(cwp+"scribe.flushperiod", "Scribe buffer flush period (milliseconds)", 1000)

  val Log = Group("Logging configuration (using slf4j simpleLogger)")
  Log.string(sl+"defaultLogLevel", "Default logging level", "info")
  Log.string(sl+"log.Main", "", "")
  Log.string(sl+"log.MainService", "", "")
  Log.string(sl+"log.NewRelic", "", "")
  Log.string(sl+"log.RuleSystem", "", "")
  Log.string(sl+"log.RuleSystemLoader", "", "")
  Log.string(sl+"log.Scribe", "", "")
}



object Main extends App {
  val versionString = s"Phalanx server version ${PhalanxVersion.version}"


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
	def scribeClient() = new ScribeClient("log_phalanx", Config.scribeHost(), Config.scribePort())

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
		case None => ()
	}
	val logger = NiceLogger("Main")
  logger.info(versionString+ " starting")
  logger.info(cfName match {
    case Some(fileName) => s"Loaded configuration from $fileName"
    case None => "No configuration file specified, using defaults."
  })

  if (Config.preloadClasses()) {
    val preloaded = PackagePreloader(this.getClass, Seq(
      "com.wikia.phalanx",
      "com.twitter.finagle.http",
      "com.twitter.util",
      "net.szumo.fstl"
    ))
    logger.info("Preloaded " + preloaded.size + " classes")
  }

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
    .maxConcurrentRequests(Config.maxConcurrentRequests())
		.sendBufferSize(Config.sendBufferSize())
		.recvBufferSize(Config.recvBufferSize())
    .cancelOnHangup(Config.cancelOnHangup())
		.backlog(Config.backlog())
    .keepAlive(Config.keepAlive())
    .hostConnectionMaxIdleTime(com.twitter.util.Duration.fromMilliseconds((Config.maxIdleTime()*1000).toLong))
		.bindTo(new java.net.InetSocketAddress(port))

  def buildService = {
    sys.props.get("newrelic.environment") match {
      case Some(x) if Config.newRelic() => ExceptionFilter andThen NewRelic andThen mainService
      case _ => ExceptionFilter andThen mainService
    }
  }

	val server = config.build(buildService)
	logger.info(s"Listening on port: $port")
	logger.trace("Initial stats: \n" + mainService.statsString)

	sys.addShutdownHook {
		logger.warn("Terminating")
		server.close(3.seconds)
		mainService.close()
		logger.warn("Shutdown complete")
	}
}

