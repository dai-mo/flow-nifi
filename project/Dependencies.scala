import sbt._

object Dependencies {
  lazy val scVersion = "2.11.7"

  // Versions

  lazy val dcsCommonsVersion = "0.2.0-SNAPSHOT"
  lazy val dcsApiVersion     = "0.3.0-SNAPSHOT"
  lazy val dcsRemoteVersion  = "0.3.0-SNAPSHOT"
  lazy val dcsCoreVersion    = "0.2.0-SNAPSHOT"
  lazy val nifiVersion			 = "1.0.0-BETA"
  lazy val slf4jVersion			 = "1.7.12"
  lazy val jsonPathVersion	 = "1.2.0"
  lazy val logbackVersion    = "1.1.3"
  lazy val rxScalaVersion    = "0.26.1"
  lazy val playVersion			 = "2.5.3"
  lazy val jerseyVersion  	 = "2.22.1"
  lazy val mockitoVersion    = "1.10.19"
  lazy val scalaTestVersion  = "2.2.6"
  lazy val juiVersion        = "0.11"
  lazy val paxCdiVersion     = "0.12.0"

  val dcsNifiServApi  = "org.dcs"                          % "org.dcs.nifi.services-api"
  val dcsNifiServices = "org.dcs"                  			   % "org.dcs.nifi.services"

  val dcsCommons          = "org.dcs"                          % "org.dcs.commons"                    % dcsCommonsVersion
  val dcsApi          = "org.dcs"                          % "org.dcs.api"                        % dcsApiVersion
  val dcsRemote    		= "org.dcs"                          % "org.dcs.remote"                     % dcsRemoteVersion

  val nifiApi         = "org.apache.nifi"                  % "nifi-api"                    				% nifiVersion
  val nifiProcUtils   = "org.apache.nifi"                  % "nifi-processor-utils"        				% nifiVersion
  val nifiClientDTO	  = "org.apache.nifi"									 % "nifi-client-dto"										% nifiVersion
  val nifiMock        = "org.apache.nifi"                  % "nifi-mock"                  				% nifiVersion
  val jsonPath        = "com.jayway.jsonpath"      			   % "json-path"                  				% jsonPathVersion
  val scalaLib				= "org.scala-lang"									 % "scala-library"                      % scVersion
  val slf4jSimple     = "org.slf4j"                  			 % "slf4j-simple"                				% slf4jVersion
  val logbackCore     = "ch.qos.logback"                   % "logback-core"                       % logbackVersion
  val logbackClassic  =	"ch.qos.logback"                   % "logback-classic"                    % logbackVersion
  val dcsCore         = "org.dcs"                          % "org.dcs.core"                       % dcsCoreVersion

  val mockitoCore     = "org.mockito"                      % "mockito-core"                       % mockitoVersion
  val mockitoAll      = "org.mockito"                      % "mockito-all"                        % mockitoVersion
  val scalaTest       = "org.scalatest"                    %% "scalatest"                         % scalaTestVersion
  val junitInterface  = "com.novocode"                     % "junit-interface"                    % juiVersion
  val jerseyClient    = "org.glassfish.jersey.core"        % "jersey-client"                      % jerseyVersion
  val jerseyMoxy      = "org.glassfish.jersey.media"       % "jersey-media-moxy"                  % jerseyVersion
  val paxCdiApi       = "org.ops4j.pax.cdi"                % "pax-cdi-api"                        % paxCdiVersion

  // Collect Processors Dependencies
  val processorsDependencies = Seq(
    dcsCommons,
    dcsApi,
    dcsRemote,
    nifiApi,
    nifiProcUtils,
    jsonPath,

    paxCdiApi       % "test",
    slf4jSimple     % "test",
    dcsCore         % "test",
    nifiMock				% "test",
    mockitoCore     % "test",
    mockitoAll      % "test",
    scalaTest       % "test",
    junitInterface  % "test"
  )

  // Collect Services Api Dependencies
  val servicesApiDependencies = Seq(
    nifiApi % "provided"
  )

  // Collect Services Dependencies
  val servicesDependencies = Seq(
    dcsCommons,
    dcsApi,
    dcsRemote,
    nifiProcUtils,
    scalaLib,

    nifiApi % "provided",

    slf4jSimple     % "test",
    dcsCore         % "test",
    nifiMock				% "test",
    mockitoCore     % "test",
    mockitoAll      % "test",
    scalaTest       % "test",
    junitInterface  % "test"
  )

  // Collect Rest Client Dependencies
  val flowDependencies = Seq(
    dcsCommons,
    dcsApi,
    jerseyClient,
    jerseyMoxy,
    nifiClientDTO	,
    logbackCore     % "provided",
    logbackClassic  % "provided",

    mockitoCore     % "test",
    mockitoAll      % "test",
    scalaTest       % "test",
    junitInterface  % "test"
  )
}
