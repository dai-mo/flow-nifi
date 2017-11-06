DCS Kaa IoT Client
==================

This project provides,
 * Automated setup of a blank Kaa IoT Platform with credentials and sample applications.
 * Test clients which simulate IoT devices.

Automated Setup
---------------
The automated setup of a blank Kaa IoT Platform includes,
* creation of DCS Superuser
* creation of DCS Tenant user
* creation of DCS Tenant Admin and Tenant Dev users
* creation of [sample](http://kaaproject.github.io/kaa/docs/v0.10.0/Programming-guide/Your-first-Kaa-application/) application along with corresponding log / configuration schema as well as log appender
* creation of a Heartbeat Monitor application with no log appender  


The setup requires a credentials file - _kaaCredentials.yaml_ and a directory containing the following files,
* _applications.yaml_ : containing application configuration details
* other files (log / configuration schema, log appender config) which are referenced from the above files

A sample credentials file is available at _src/test/resources/kaaCredentials.yaml_
A sample directory is available at _src/test/resources/kaa-config_

To execute the setup run,  
`sbt -DkaaCredentials=</path/to/DCS Kaa Credentials File> -DkaaConfigDir=</path/to/DCS Kaa Config Directory> "kaaiot-client/runMain org.dcs.iot.kaa.KaaIoTInitialiser"`  
in the root directory of the parent _dcs_nifi_ project

Client Applications
-------------------

### Data Collection Demo
This client corresponds to the [Data Collection](http://kaaproject.github.io/kaa/docs/v0.10.0/Programming-guide/Your-first-Kaa-application/) application.

To run this client,
 * Add an SDK Profile to the target application and generate / download the SDK as described [here](http://kaaproject.github.io/kaa/docs/v0.10.0/Programming-guide/Your-first-Kaa-application/#generate-sdk)
 * Create _lib_ directory in this project and copy the downloaded jar to this directory
 * Copy the _DataCollectionClient.scala_ file to the _src/main/scala/org/dcs/iot/kaa/client_ directory
 * Execute `sbt "kaaiot-client/runMain org.dcs.iot.kaa.client.DataCollectionClient"` in the root directory of the parent dcs_nifi project.
