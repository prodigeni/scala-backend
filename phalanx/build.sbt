import AssemblyKeys._ // sbt-assembly

assemblySettings

name := "phalanx"

organization := "com.wikia"

version := "0.19"

version ~= { (s) => {
  import scala.sys.process._
  val result = Seq("git", "rev-list", "--max-count=1" , "HEAD").!!.stripLineEnd // runs git in shell
  if (result.size == 40)  s + "." + result else s
} }

scalaVersion := "2.10.0"

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
  "com.logentries.re2" % "libre2-java" % "1.0",
  "com.wikia" %% "wikifactory" % "0.10",
  "com.typesafe.slick" %% "slick" % "1.0.0",
  "com.twitter" % "finagle-core_2.10" % "6.1.1",
  "com.twitter" % "finagle-http_2.10" % "6.1.1",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.2",
  "newrelic.java-agent" % "newrelic-api" % "2.7.0"
)

scalacOptions ++= Seq("-deprecation", "-unchecked")

artifact in (Compile, assembly) ~= { art =>
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

assembly ~= { (f)  => {
  import scala.sys.process._
  Seq("cp", f.getPath, "deploy/phalanx-server.jar").! // runs cp in shell
  f }
}

seq(ReflectPlugin.allSettings:_*)

reflectPackage	:= "com.wikia.phalanx"

reflectClass	:= "PhalanxVersion"

sourceGenerators in Compile <+= reflect map identity
