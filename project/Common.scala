
import sbt._
import Keys._

import Dependencies._
import Global._

object Common {
	lazy val commonSettings = Seq(
			organization := "org.dcs",
			version := dcsNifiVersion,
			scalaVersion := scVersion,
			crossPaths := false,
			checksums in update := Nil,
			javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
			javacOptions in doc := Seq("-source", "1.8"),
			resolvers ++= Seq(
			    DefaultMavenRepository,
					localMavenRepository
					)
			)
}
