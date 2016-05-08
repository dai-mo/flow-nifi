import sbt._

object Dependencies {

	    // Versions
	    lazy val dcsApiVersion    = "1.0.0-SNAPSHOT"
	    lazy val dcsRemoteVersion= "1.0.0-SNAPSHOT"
			lazy val nifiVersion			= "0.5.1"
			lazy val slf4jVersion			= "1.7.12"
			lazy val jsonPathVersion	= "1.2.0"

			lazy val dcsNifiServicesVersion = "0.0.1-SNPSHOT"

			lazy val dcsCoreVersion   = "1.0.0-SNAPSHOT"
			lazy val mockitoVersion   = "1.10.19"
			lazy val scalaTestVersion = "2.2.6"
			lazy val juiVersion       = "0.11"



			// Required dependencies
			val dcsApi          = "org.dcs"                          % "org.dcs.api"                        % dcsApiVersion
			val dcsRemote    		 = "org.dcs"                          % "org.dcs.remote"                    % dcsRemoteVersion
			val nifiApi         = "org.apache.nifi"                  % "nifi-api"                    				% nifiVersion
			val nifiProcUtils   = "org.apache.nifi"                  % "nifi-processor-utils"        				% nifiVersion
			val jsonPath        = "com.jayway.jsonpath"      			   % "json-path"                  				% jsonPathVersion

			// Provided Dependencies
			val dcsNifiServices = "org.dcs"                  			   % "org.dcs.nifi.services"       				% dcsNifiServicesVersion

			// Test Dependencies
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
					mockitoCore     % "test",
					mockitoAll      % "test",
					scalaTest       % "test",
					junitInterface  % "test"
					)
}
