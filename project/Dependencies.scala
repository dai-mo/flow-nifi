import sbt._
import Global._

object Dependencies {

			lazy val dcsNifiVersion   = "0.0.1-SNAPSHOT"

	    // Versions
	    lazy val dcsApiVersion    = "1.0.0-SNAPSHOT"
	    lazy val dcsRemoteVersion = "1.0.0-SNAPSHOT"
			lazy val dcsCoreVersion   = "1.0.0-SNAPSHOT"
			lazy val nifiVersion			= "0.5.1"
			lazy val slf4jVersion			= "1.7.12"
			lazy val jsonPathVersion	= "1.2.0"

			lazy val mockitoVersion   = "1.10.19"
			lazy val scalaTestVersion = "2.2.6"
			lazy val juiVersion       = "0.11"

			val dcsApi          = "org.dcs"                          % "org.dcs.api"                        % dcsApiVersion
			val dcsRemote    		= "org.dcs"                          % "org.dcs.remote"                     % dcsRemoteVersion
			val dcsNifiServApi  = "org.dcs"                          % "org.dcs.nifi.services-api"          % dcsNifiVersion
			val nifiApi         = "org.apache.nifi"                  % "nifi-api"                    				% nifiVersion
			val nifiProcUtils   = "org.apache.nifi"                  % "nifi-processor-utils"        				% nifiVersion
			val nifiMock        = "org.apache.nifi"                  % "nifi-mock"                  				% nifiVersion
			val jsonPath        = "com.jayway.jsonpath"      			   % "json-path"                  				% jsonPathVersion
			val dcsNifiServices = "org.dcs"                  			   % "org.dcs.nifi.services"       				% dcsNifiVersion
			val scalaLib				= "org.scala-lang"									 % "scala-library"                      % scVersion
			val slf4jSimple     = "org.slf4j"                  			 % "slf4j-simple"                				% slf4jVersion
			val dcsCore         = "org.dcs"                          % "org.dcs.core"                       % dcsCoreVersion
			val mockitoCore     = "org.mockito"                      % "mockito-core"                       % mockitoVersion
			val mockitoAll      = "org.mockito"                      % "mockito-all"                        % mockitoVersion
			val scalaTest       = "org.scalatest"                    %% "scalatest"                         % scalaTestVersion
			val junitInterface  = "com.novocode"                     % "junit-interface"                    % juiVersion

			// Collect Processors Dependencies
			val processorsDependencies = Seq(
					dcsApi,
					dcsRemote,
					nifiApi,
					nifiProcUtils,
					jsonPath,

					dcsNifiServices % "provided",

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
					dcsNifiServApi,
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
}
