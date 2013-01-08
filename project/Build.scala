import sbt._
import Keys._

object BackendBuild extends Build {
    lazy val root = Project(id = "backend",
                            base = file(".")) aggregate( wikifactory, phalanx )

    lazy val wikifactory = Project(id = "backend-wikifactory",
                           base = file("wikifactory"))

    lazy val phalanx = Project(id = "backend-phalanx",
                           base = file("phalanx"))
}
