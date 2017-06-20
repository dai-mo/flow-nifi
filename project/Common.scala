
import sbt._
import Keys._

import Dependencies._
import sbt._
import Keys._

object Common {

	lazy val UNIT = config("unit") extend Test
	lazy val IT = config("it") extend Test


	lazy val commonSettings = Seq(
			organization := "org.dcs",
			scalaVersion := "2.11.7",
			crossPaths := false,
			checksums in update := Nil,
			javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
			javacOptions in doc := Seq("-source", "1.8")
			)

	def BaseProject(projectID: String, projectName: String) =
		    Project(projectID, file(projectName)).
			  configs(IT).
			  settings(inConfig(IT)(Defaults.testTasks): _*).
			  settings(testOptions in IT := Seq(Tests.Argument("-n", "IT"))).
			  configs(UNIT).
			  settings(inConfig(UNIT)(Defaults.testTasks): _*).
			  settings(testOptions in UNIT := Seq(
						Tests.Argument("-l", "IT"),
						Tests.Argument("-l", "E2E")
					)
				)
}
