package com.wikia.phalanx

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.Future
import org.apache.thrift.TBase
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TMemoryBuffer
import scribe.thrift.LogEntry
import util.parsing.json.{JSONFormat, JSONObject}


class Scribe(host: String = "localhost", port:Int = 1463) extends Service[(String, Map[String, Any]), Unit] {
	private val factory = new org.apache.thrift.protocol.TBinaryProtocol.Factory()
	private val thrift: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
		.hosts(host+":"+port)
		.hostConnectionLimit(1)
		.codec(ThriftClientFramedCodec())
		.build()

	override def release() {
		thrift.release()
	}

	def category(cat: String):ScribeWriter = map((msg: ScribeMap) => (cat, msg))

	def apply(request: (String, Map[String, Any])): Future[Unit] = {
		val cat: String = request._1
		val json = JSONObject(request._2).toString(JSONFormat.defaultFormatter)
		val entry = new LogEntry(cat, json)
		val bytes = messageToArray(entry, factory)
		val thriftRequest = new ThriftClientRequest(bytes, false)
		thrift(thriftRequest).map(x => ())
	}
	private def messageToArray(message: TBase[_, _], protocolFactory: TProtocolFactory) = {
		// adapted from finagle-thrift OutputBuffer, which is unfortunately private
		val memoryBuffer = new TMemoryBuffer(256)
		val oprot = protocolFactory.getProtocol(memoryBuffer)
		message.write(oprot)
		oprot.getTransport.flush()
		java.util.Arrays.copyOfRange(memoryBuffer.getArray, 0, memoryBuffer.length())
	}
}

class ScribeBuffer extends ScribeWriter {
	val requests = scala.collection.mutable.ArrayBuffer.empty[Map[String, Any]]
	def apply(request: Map[String, Any]): Future[Unit] = {
		requests += request
		Future.Done
	}
	def flushTo(other: ScribeWriter): Future[Unit] = {
	  val result = requests.map { other(_) }
		requests.clear()
		Future.join(result)
	}
}