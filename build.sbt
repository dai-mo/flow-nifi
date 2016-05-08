import Dependencies._
import Common._

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "org.dcs.nifi.parent"
  ).
  aggregate(processors)

lazy val processorsProjectName = "org.dcs.nifi.processors"
lazy val processorsProjectID   = "processors"

lazy val processors = (
  Project(processorsProjectID, file(processorsProjectName)).
  settings(commonSettings: _*).
  settings(
    name := processorsProjectName,
    moduleName := processorsProjectName,
    libraryDependencies ++= processorsDependencies
  )
)
