package org.dcs.nifi.repository

import org.dcs.commons.config.DbConfig
import org.flywaydb.core.Flyway
import org.postgresql.jdbc3.Jdbc3PoolingDataSource

/**
  * Created by cmathew on 19.01.17.
  */
object DbMigration {
  import org.dcs.commons.config.DbConfig._

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
