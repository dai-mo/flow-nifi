import Dependencies._
import Common._
import com.typesafe.sbt.GitPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease._

val license = Some(HeaderLicense.Custom(
  """Copyright (c) 2017-2018 brewlabs SAS
    |
    |Licensed under the Apache License, Version 2.0 (the "License");
    |you may not use this file except in compliance with the License.
    |You may obtain a copy of the License at
    |
    |    http://www.apache.org/licenses/LICENSE-2.0
    |
    |Unless required by applicable law or agreed to in writing, software
    |distributed under the License is distributed on an "AS IS" BASIS,
    |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    |See the License for the specific language governing permissions and
    |limitations under the License.
    |
    |""".stripMargin
))


val projectName = "org.dcs.nifi.parent"

lazy val dcsnifi = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := projectName,
    headerLicense := license
  ).aggregate(servicesapi, services, processors, flow, repo)



lazy val servicesApiProjectName = "org.dcs.nifi.services-api"
lazy val servicesApiProjectID   = "servicesapi"

lazy val servicesapi =
  BaseProject(servicesApiProjectID, servicesApiProjectName).
    settings(commonSettings: _*).
    settings(
      name := servicesApiProjectName,
      moduleName := servicesApiProjectName,
      libraryDependencies ++= servicesApiDependencies,
      headerLicense := license
    )


lazy val servicesProjectName = "org.dcs.nifi.services"
lazy val servicesProjectID   = "services"

lazy val services =
  BaseProject(servicesProjectID, servicesProjectName).
    settings(commonSettings: _*).
    dependsOn(servicesapi).
    settings(
      name := servicesProjectName,
      moduleName := servicesProjectName,
      libraryDependencies ++= (Seq(dcsNifiServApi % version.value) ++ servicesDependencies),
      headerLicense := license
    )


lazy val flowProjectName = "org.dcs.flow"
lazy val flowProjectID   = "flow"

lazy val flow =
  BaseProject(flowProjectID, flowProjectName).
    settings(commonSettings: _*).
    settings(
      name := flowProjectName,
      moduleName := flowProjectName,
      libraryDependencies ++= flowDependencies,
      headerLicense := license
    )



lazy val processorsProjectName = "org.dcs.nifi.processors"
lazy val processorsProjectID   = "processors"

lazy val processors =
  BaseProject(processorsProjectID, processorsProjectName).
    settings(commonSettings: _*).
    dependsOn(services).
    settings(
      name := processorsProjectName,
      moduleName := processorsProjectName,
      libraryDependencies ++= (Seq(dcsNifiServices % version.value % "provided") ++ processorsDependencies),
      headerLicense := license
    )


lazy val repoProjectName = "org.dcs.nifi.repo"
lazy val repoProjectID   = "repo"

lazy val repo =
  BaseProject(repoProjectID, repoProjectName).
    settings(commonSettings: _*).
    settings(
      name := repoProjectName,
      moduleName := repoProjectName,
      libraryDependencies ++= repoDependencies,
      headerLicense := license
    )

resolvers += Resolver.mavenLocal

// ------- Versioning , Release Section --------

// Build Info
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := projectName

// Git
showCurrentGitBranch

git.useGitDescribe := true

git.baseVersion := "0.0.0"

val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r

git.gitTagToVersionNumber := {
  case VersionRegex(v,"SNAPSHOT") => Some(s"$v-SNAPSHOT")
  case VersionRegex(v,"") => Some(v)
  case VersionRegex(v,s) => Some(s"$v-$s-SNAPSHOT")
  case v => None
}

lazy val bumpVersion = settingKey[String]("Version to bump - should be one of \"None\", \"Major\", \"Patch\"")
bumpVersion := "None"

releaseVersion := {
  ver => bumpVersion.value.toLowerCase match {
    case "none" => Version(ver).
      map(_.withoutQualifier.string).
      getOrElse(versionFormatError)
    case "major" => Version(ver).
      map(_.withoutQualifier).
      map(_.bump(sbtrelease.Version.Bump.Major).string).
      getOrElse(versionFormatError)
    case "patch" => Version(ver).
      map(_.withoutQualifier).
      map(_.bump(sbtrelease.Version.Bump.Bugfix).string).
      getOrElse(versionFormatError)
    case _ => sys.error("Unknown bump version - should be one of \"None\", \"Major\", \"Patch\"")
  }
}

releaseVersionBump := sbtrelease.Version.Bump.Minor
