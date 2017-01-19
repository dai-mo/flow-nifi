package org.dcs.nifi.repository

import java.util.Properties

/**
  * Created by cmathew on 19.01.17.
  */
object DbConfig {
  val DbPropertyFileName = "application.properties"
  val DbPostgresPrefix = "postgres"


  val properties : Properties  = {
    val dbProperties = new Properties()
    val dbPropertiesIS = getClass.getClassLoader.getResourceAsStream(DbPropertyFileName)
    if(dbPropertiesIS != null)
      dbProperties.load(dbPropertiesIS)
    else
      throw new IllegalStateException("Could not load properties from file : " + DbPropertyFileName)
    dbProperties
  }
}


