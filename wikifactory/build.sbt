name := "wikifactory"

version := "0.1"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq( 
	"org.yaml" % "snakeyaml" % "1.11",
	"org.scalatest" % "scalatest" % "1.3" % "test",
	"mysql" % "mysql-connector-java" % "5.1.10"
)
