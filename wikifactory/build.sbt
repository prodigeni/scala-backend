name := "wikifactory"

organization := "com.wikia"

version := "0.4"

scalaVersion := "2.9.2"

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

resolvers ++= Seq( 
	"Sonatype Maven repository" at "https://oss.sonatype.org/content/repositories/snapshots/org/jooq/",
	"Wikia Maven repository" at "http://pkg-s1.wikia-prod/maven/releases/"
)

libraryDependencies ++= Seq(
	"org.jooq" % "jooq" % "2.6.1",
	"org.yaml" % "snakeyaml" % "1.11",
	"org.apache.logging.log4j" % "slf4j-impl" % "2.0-alpha2",
	"org.scalatest" %% "scalatest" % "1.8" % "test",
	"mysql" % "mysql-connector-java" % "5.1.22",
	"org.apache.thrift" % "libthrift" % "0.9.0",
	"org.apache.thrift" % "libfb303" % "0.9.0",
	"com.ning" % "metrics.eventtracker-scribe" % "4.1.2"
)
