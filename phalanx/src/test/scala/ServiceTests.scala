package com.wikia.phalanx.tests

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ServerBuilder, ClientBuilder}
import com.twitter.finagle.http._
import com.wikia.phalanx._
import java.net.InetSocketAddress
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.scalatest.FlatSpec

class ServiceTests extends FlatSpec  {
	val charset = Charset.forName("utf-8")

	val rules = DataTests.rules

	val service = new MainService(Map( ("tests", new RuleSystem(rules).combineRules )))
	val config =  ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.bindTo(new java.net.InetSocketAddress(0))

	val server = config.build(service)
  val port =  server.localAddress match {
		case addr:InetSocketAddress => addr.getPort
	}

	val address = "http://localhost:"+port+"/"

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

	"Check" should "work with POST" in {
		val request = RequestBuilder().url(address+"check").addFormElement( ("type", "tests"), ("content", "fuck") ).buildFormPost()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "failure")

	}

	it should "work with GET" in {
		val request = RequestBuilder().url(address+"check?type=tests&content=fuck").buildGet()
		val response = client(request)()
		val content = response.getContent.toString(charset)
		assert(content === "failure")

	}

	"Server" should "shutdown" in {
		server.close()
	}
}
