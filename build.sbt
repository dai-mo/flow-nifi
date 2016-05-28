import Dependencies._
import Common._

lazy val dcsnifi = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "org.dcs.nifi.parent"
  ).aggregate(servicesapi, services, processors)



lazy val servicesApiProjectName = "org.dcs.nifi.services-api"
lazy val servicesApiProjectID   = "servicesapi"

lazy val servicesapi = (
  BaseProject(servicesApiProjectID, servicesApiProjectName).
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
  BaseProject(servicesProjectID, servicesProjectName).
  settings(commonSettings: _*).
  dependsOn(servicesapi).
  settings(
    name := servicesProjectName,
    moduleName := servicesProjectName,
    libraryDependencies ++= servicesDependencies
  )
)


lazy val processorsProjectName = "org.dcs.nifi.processors"
lazy val processorsProjectID   = "processors"

lazy val processors = (
  BaseProject(processorsProjectID, processorsProjectName).
  settings(commonSettings: _*).
  dependsOn(services).
  settings(
    name := processorsProjectName,
    moduleName := processorsProjectName,
    libraryDependencies ++= processorsDependencies
  )
)
