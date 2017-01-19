package org.dcs.nifi.repository

import java.util.Properties

import org.flywaydb.core.Flyway
import org.postgresql.jdbc3.Jdbc3PoolingDataSource

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

object DbMigration {
  import DbConfig._

  def migratePostgres(): Unit = {
    // Create the Flyway instance
    val flyway: Flyway = new Flyway()

    val host = DbConfig.properties.getProperty(DbPostgresPrefix + ".dataSource.serverName")
    val port = DbConfig.properties.getProperty(DbPostgresPrefix + ".dataSource.portNumber")
    val database = DbConfig.properties.getProperty(DbPostgresPrefix + ".dataSource.databaseName")
    val user = DbConfig.properties.getProperty(DbPostgresPrefix + ".dataSource.user")
    val password = DbConfig.properties.getProperty(DbPostgresPrefix + ".dataSource.password")

    // Point it to the database
    val datasource: Jdbc3PoolingDataSource = new Jdbc3PoolingDataSource()
    datasource.setDataSourceName("Postgres Data Source")
    datasource.setServerName(host)
    datasource.setDatabaseName(database)
    datasource.setUser(user)
    datasource.setPassword(password)
    //val datasource = "jdbc:postgres://" + host + ":" + port + "/" + database
    flyway.setDataSource(datasource)

    flyway.setLocations("db/migration/postgres")

    // Start the migration
    flyway.migrate()
  }
}
