name := "wikifactory"

version := "0.1"

scalaVersion := "2.10.0-RC5"

libraryDependencies ++= Seq( 
	"org.yaml" % "snakeyaml" % "1.11",
	"org.scalatest" % "scalatest_2.10.0-RC5" % "2.0.M5-B1" % "test",
	"mysql" % "mysql-connector-java" % "5.1.10",
	"com.twitter" % "querulous" % "1.1.0"
)
