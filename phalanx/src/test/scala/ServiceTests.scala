package com.wikia.phalanx.tests

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.finagle.http._
import com.wikia.phalanx._
import java.net.InetSocketAddress
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.scalatest.FlatSpec

class ServiceTests extends FlatSpec {
	val charset = Charset.forName("utf-8")

	val rules = DataTests.rules
	val scribe = new ScribeBuffer().category("tests")
	val service = new MainService(Map(("tests", new FlatRuleSystem(rules).combineRules)), (x,y) => x, scribe)
	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.bindTo(new java.net.InetSocketAddress(0))

	val server = config.build(service)
	val port = server.localAddress match {
		case addr: InetSocketAddress => addr.getPort
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
		val request = RequestBuilder().url(address + "check").addFormElement(("type", "tests"), ("content", "fuckąężźćńół")).buildFormPost()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "failure\n")

	}

	it should "work with GET" in {
		val request = RequestBuilder().url(address + "check?type=tests&content=fuck").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "failure\n")
	}

	it should "return good JSON for /match" in {
		val request = RequestBuilder().url(address + "match?type=tests&content=fuck").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "[{\"regex\" : true, \"expires\" : \"\", \"text\" : \"^fuck\", \"reason\" : \"\", \"exact\" : false, \"caseSensitive\" : false, \"id\" : 589, \"language\" : \"\", \"authorId\" : 0}]")
	}

	it should "support multiple content parameters" in {
		val request = RequestBuilder().url(address + "check?type=tests&content=ok&content=fuck&content=something").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "failure\n")
	}

	it should "provide stats" in {
		val request = RequestBuilder().url(address + "status").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content != "failure\n")
	}

	"Server" should "shutdown" in {
		server.close()
	}
}
