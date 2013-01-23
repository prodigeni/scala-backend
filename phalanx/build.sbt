import AssemblyKeys._ // for sbt-assembly

assemblySettings

name := "phalanx"

organization := "com.wikia"

version := "0.3"

scalaVersion := "2.9.2"

resolvers += "twitter" at "http://maven.twttr.com"

resolvers += "scala-tools-releases" at "https://repository.jboss.org/nexus/content/repositories/scala-tools-releases"

resolvers += "scala-tools.org" at "http://scala-tools.org/repo-releases"

resolvers += "Wikia Maven repository" at "http://pkg-s1.wikia-prod/maven/releases/"

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
  "com.wikia" %% "wikifactory" % "0.5",
  "com.twitter" % "finagle-core" % "5.3.1",
  "com.twitter" % "finagle-http" % "5.3.1",
  "org.scalatest" %% "scalatest" % "1.8" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.2" % "compile"
)


artifact in (Compile, assembly) ~= { art => {
  art.copy(`classifier` = Some("assembly"))
  }
}

addArtifact(artifact in (Compile, assembly), assembly)

