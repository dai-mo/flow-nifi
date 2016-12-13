import sbt._

object Dependencies {
  lazy val scVersion = "2.11.7"

  // Versions

  lazy val dcsCommonsVersion      = "0.2.0-SNAPSHOT"
  lazy val dcsApiVersion          = "0.3.0-SNAPSHOT"
  lazy val dcsRemoteVersion       = "0.3.0-SNAPSHOT"
  lazy val dcsCoreVersion         = "0.2.0-SNAPSHOT"
  lazy val playVersion			      = "2.5.3"
  lazy val nifiVersion			      = "1.0.0-BETA"
  lazy val slf4jVersion			      = "1.7.12"
  lazy val jsonPathVersion	      = "1.2.0"
  lazy val logbackVersion         = "1.1.3"
  lazy val rxScalaVersion         = "0.26.1"
  lazy val akkaVersion            = "2.4.4"
  lazy val quillVersion           = "1.0.0"
  lazy val quillJdbcVersion       = "1.0.1"
  lazy val dataStaxDriverVersion  = "3.1.0"
  lazy val mysqlConnectorVersion  = "5.1.38"
  lazy val apacheCommonsVersion   = "1.3.2"



  lazy val mockitoVersion         = "1.10.19"
  lazy val scalaTestVersion       = "2.2.6"
  lazy val juiVersion             = "0.11"
  lazy val paxCdiVersion          = "0.12.0"

  val dcsNifiServApi  = "org.dcs"                          % "org.dcs.nifi.services-api"
  val dcsNifiServices = "org.dcs"                  			   % "org.dcs.nifi.services"

  val dcsCommons      = "org.dcs"                          % "org.dcs.commons"                    % dcsCommonsVersion
  val dcsApi          = "org.dcs"                          % "org.dcs.api"                        % dcsApiVersion
  val dcsRemote    		= "org.dcs"                          % "org.dcs.remote"                     % dcsRemoteVersion

  val apacheCommons   = "org.apache.commons"               % "commons-io"                         % apacheCommonsVersion
  val nifiApi         = "org.apache.nifi"                  % "nifi-api"                    				% nifiVersion
  val nifiProcUtils   = "org.apache.nifi"                  % "nifi-processor-utils"        				% nifiVersion
  val nifiClientDTO	  = "org.apache.nifi"									 % "nifi-client-dto"										% nifiVersion
  val nifiFrameworkApi= "org.apache.nifi"									 % "nifi-framework-api"								  % nifiVersion
  val nifiUtils       = "org.apache.nifi"									 % "nifi-utils"                         % nifiVersion
  val nifiMock        = "org.apache.nifi"                  % "nifi-mock"                  				% nifiVersion
  val jsonPath        = "com.jayway.jsonpath"      			   % "json-path"                  				% jsonPathVersion
  val scalaLib				= "org.scala-lang"									 % "scala-library"                      % scVersion
  val slf4jSimple     = "org.slf4j"                  			 % "slf4j-simple"                				% slf4jVersion
  val logbackCore     = "ch.qos.logback"                   % "logback-core"                       % logbackVersion
  val logbackClassic  =	"ch.qos.logback"                   % "logback-classic"                    % logbackVersion
  val dcsCore         = "org.dcs"                          % "org.dcs.core"                       % dcsCoreVersion
  val quillCassandra  = "io.getquill"                      %% "quill-cassandra"                   % quillVersion
  val quillJdbc       = "io.getquill"                      %% "quill-jdbc"                        % quillJdbcVersion
  val datastaxDriver  = "com.datastax.cassandra"           % "cassandra-driver-core"              % dataStaxDriverVersion
  val mysqlDriver     = "mysql"                            % "mysql-connector-java"               % mysqlConnectorVersion

  val playWs          = "com.typesafe.play"                %% "play-ws"                           % playVersion
  val mockitoCore     = "org.mockito"                      % "mockito-core"                       % mockitoVersion
  val mockitoAll      = "org.mockito"                      % "mockito-all"                        % mockitoVersion
  val scalaTest       = "org.scalatest"                    %% "scalatest"                         % scalaTestVersion
  val junitInterface  = "com.novocode"                     % "junit-interface"                    % juiVersion
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
    nifiFrameworkApi,

    scalaLib,

    nifiApi         % "provided",

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
    nifiClientDTO,

    logbackCore     % "provided",
    logbackClassic  % "provided",

    mockitoCore     % "test",
    mockitoAll      % "test",
    scalaTest       % "test",
    junitInterface  % "test"
  )

  // Collect Repo Dependencies
  val repoDependencies = Seq(
    quillCassandra,
    //quillJdbc,
    datastaxDriver,
    //mysqlDriver,
    apacheCommons,
    scalaLib,

    nifiUtils        % "provided",
    nifiFrameworkApi % "provided",
    nifiApi          % "provided",

    mockitoCore      % "test",
    mockitoAll       % "test",
    scalaTest        % "test",
    junitInterface   % "test"
  )
}
