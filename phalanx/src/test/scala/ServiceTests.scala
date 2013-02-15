package com.wikia.phalanx.tests

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.finagle.http._
import com.wikia.phalanx._
import java.net.InetSocketAddress
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.scalatest.FlatSpec
import com.twitter.util.Future

class ScribeMock extends ScribeLike {
	val stored = collection.mutable.Buffer.empty[Map[String,Any]]
	def apply(request: Map[String, Any]): Future[Unit] = {
		stored += request
		Future.Done
	}
	def many(request: Seq[Map[String, Any]]): Future[Unit] = {
		stored ++= request
		Future.Done
	}
	def clear() {
		stored.clear()
	}
}

class ServiceTests extends FlatSpec {
	val charset = Charset.forName("utf-8")

	val rules = DataTests.rules
	val scribe = new ScribeMock()
	val service = new MainService(Map(("tests", new CombinedRuleSystem(rules))), scribe)
	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.bindTo(new java.net.InetSocketAddress(0))

	val server = config.build(service)
	val port = server.localAddress match {
		case addr: InetSocketAddress => addr.getPort
	}

	def checkScribe(block: => Unit, count:Int = 1 ) {
		scribe.clear()
		assert(scribe.stored.isEmpty)
		block
		assert(scribe.stored.size === count)
	}

	val address = "http://localhost:" + port + "/"

	val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
		.codec(Http())
		.hosts(server.localAddress)
		.hostConnectionLimit(1)
		.build()

	"Server" should "be running" in {
		val request = RequestBuilder().url(address).buildGet
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "PHALANX ALIVE")
	}

	it should "work with POST" in {
		checkScribe {
			val request = RequestBuilder().url(address + "check?user=ala&wiki=1").addFormElement(("type", "tests"), ("content", "fuckąężźćńół")).buildFormPost()
			val response = client(request)()
			val content = response.getContent.toString(charset)
			assert(content === "failure\n")
		}
	}

	it should "work with GET" in {
		checkScribe {
			val request = RequestBuilder().url(address + "check?user=ala&wiki=1&type=tests&content=fuck").buildGet()
			val response = client(request)()
			val content = response.getContent.toString(charset)
			assert(content === "failure\n")
		}
	}

	it should "work with GET, also without explicit type" in {
		checkScribe {
			val request = RequestBuilder().url(address + "check?user=ala&wiki=1&content=fuck").buildGet()
			val response = client(request)()
			val content = response.getContent.toString(charset)
			assert(content === "failure\n")
		}
	}

	it should "return good JSON for /match" in {
		checkScribe {
		val request = RequestBuilder().url(address + "match?user=ala&wiki=1&type=tests&content=fuck").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "[{\"regex\" : true, \"expires\" : \"\", \"text\" : \"^fuck\", \"reason\" : \"\", \"exact\" : false, \"caseSensitive\" : false, \"id\" : 589, \"language\" : \"\", \"authorId\" : 0, \"type\" : 8}]")
		}
	}

	it should "support multiple content parameters" in {
		checkScribe {
			val request = RequestBuilder().url(address + "check?user=ala&wiki=1&type=tests&content=ok&content=fuck&content=something").buildGet()
			val response = client(request)()
			val content = response.getContent.toString(charset)
			assert(content === "failure\n")
		}
	}

	it should "provide stats" in {
		val request = RequestBuilder().url(address + "stats").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content != "failure\n")
	}

	it should "shutdown on demand" in {
		server.close()
	}
}
