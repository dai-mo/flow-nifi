import sbt._

object Global {
  // Versions
  lazy val scVersion = "2.11.7"
  lazy val nanonetArtifactoryBaseUrl = "http://artifactory.openshift.nanonet"
  
  // Repositories
  val localMavenRepository = (
    "Local Maven Repository" at "file://" +
    Path.userHome.absolutePath +
    "/.m2/repository")

  val nanonetMavenRepository = (
    "Nanonet Maven Repository" at nanonetArtifactoryBaseUrl + "/artifactory/libs-snapshot-local/")

  val localIvyRepository = (
    "Local Ivy Repository" at "file://" +
    Path.userHome.absolutePath +
    "/.ivy2/cache")

}