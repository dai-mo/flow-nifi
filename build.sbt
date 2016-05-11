import Dependencies._
import Common._


lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "org.dcs.nifi.parent"
  ).aggregate(processors, servicesapi, services)


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

lazy val servicesApiProjectName = "org.dcs.nifi.services-api"
lazy val servicesApiProjectID   = "servicesapi"

lazy val servicesapi = (
  Project(servicesApiProjectID, file(servicesApiProjectName)).
  settings(commonSettings: _*).
  settings(
    name := servicesApiProjectName,
    moduleName := servicesApiProjectName,
    libraryDependencies ++= servicesApiDependencies
  )
)

lazy val servicesProjectName = "org.dcs.nifi.services"
lazy val servicesProjectID   = "services"

lazy val services = (
  Project(servicesProjectID, file(servicesProjectName)).
  settings(commonSettings: _*).
  dependsOn(servicesapi).
  settings(
    name := servicesProjectName,
    moduleName := servicesProjectName,
    libraryDependencies ++= servicesDependencies
  )
)
