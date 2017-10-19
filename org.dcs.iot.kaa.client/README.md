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


The setup requires a directory containing the following files,
* _credentials.json_ : containing credentials for the different users
* _application.json_ : containing application configuration details
* other files (log / configuration schema, log appender config) which are referenced from the above files

A sample directory is available at _src/test/resources/kaa-config_

To execute the setup run,  
`sbt -DkaaConfigDir=</path/to/DCS Kaa Config Directory> "kaaiot-client/runMain org.dcs.iot.kaa.KaaIoTClient"`  
in the root directory of the parent _dcs_nifi_ project

Client Applications
-------------------

### Data Collection Demo
This client corresponds to the [Data Collection](http://kaaproject.github.io/kaa/docs/v0.10.0/Programming-guide/Your-first-Kaa-application/) application.

To run this client,
 * Add an SDK Profile to the target application and generate / download the SDK as described [here](http://kaaproject.github.io/kaa/docs/v0.10.0/Programming-guide/Your-first-Kaa-application/#generate-sdk)
 * Copy the downloaded jar to the lib folder of this project
 * Execute `sbt "kaaiot-client/runMain org.dcs.iot.kaa.client.DataCollectionClient"` in the root directory of the parent dcs_nifi project.
