addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.6")

addSbtPlugin("com.wikia" % "sbt-reflect" % "1.0")

resolvers ++= Seq(
	"Wikia Maven repository" at "http://pkg-s1.wikia-prod/maven/releases/"
)
