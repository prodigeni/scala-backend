name := "phalanx"

organization := "wikia.com"

version := "0.1"

scalaVersion := "2.9.2"

resolvers += "twitter" at "http://maven.twttr.com"

resolvers += "scala-tools-releases" at "https://repository.jboss.org/nexus/content/repositories/scala-tools-releases"

resolvers += "scala-tools.org" at "http://scala-tools.org/repo-releases"

libraryDependencies ++= Seq(
  "com.twitter" % "finagle-core" % "5.3.1",
  "com.twitter" % "finagle-http" % "5.3.1",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.11",
  "org.scalatest" %% "scalatest" % "1.8" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0" % "test"
)


