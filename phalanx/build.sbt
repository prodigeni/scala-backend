name := "phalanx"

organization := "com.wikia"

version := "0.5"

scalaVersion := "2.10.0"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

resolvers ++= Seq(
	"Wikia Maven repository" at "http://pkg-s1.wikia-prod/maven/releases/",
	"NewRelic agent API" at "http://download.newrelic.com"
)

publishMavenStyle := true

publishTo <<= (version) { version: String =>
	val repoInfo = if( version.trim.endsWith( "SNAPSHOT" ) ) {
		( "Wikia Maven repository (snapshots)" -> "/srv/maven/snapshots" )
	}
	else {
		( "Wikia Maven repository (releases)" -> "/srv/maven/releases" )
	}
	val user = System.getProperty("user.name")
 	val keyFile = (Path.userHome / ".ssh" / "id_rsa").asFile
	Some( Resolver.ssh( repoInfo._1, "pkg-s1.wikia-prod", repoInfo._2 ) as ( user, keyFile ) withPermissions( "0644" ) )
}

libraryDependencies ++= Seq(
  "com.wikia" %% "wikifactory" % "0.8",
  "com.typesafe.slick" %% "slick" % "1.0.0",
  "com.twitter" % "finagle-core_2.10" % "6.1.1",
  "com.twitter" % "finagle-http_2.10" % "6.1.1",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.2",
  "newrelic.java-agent" % "newrelic-api" % "2.7.0"
)

scalacOptions ++= Seq("-deprecation", "-unchecked")

oneJar ~= { (f)  => {
  import scala.sys.process._
  Seq("cp", f.getPath, "deploy/phalanx-server.jar").! // runs cp in shell
  f }
}
