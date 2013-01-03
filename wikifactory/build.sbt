name := "wikifactory"

organization := "com.wikia"

version := "0.1"

scalaVersion := "2.9.2"

resolvers += "Sonatype Maven repository" at "https://oss.sonatype.org/content/repositories/snapshots/org/jooq/"

libraryDependencies ++= Seq( 
	"org.jooq" % "jooq" % "2.6.1",
	"org.yaml" % "snakeyaml" % "1.11",
	"org.scalatest" % "scalatest" % "1.3" % "test",
	"mysql" % "mysql-connector-java" % "5.1.22"
)
