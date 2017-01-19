package org.dcs.nifi.repository

import io.getquill.{JdbcContext, PostgresDialect, SnakeCase}

/**
  * Created by cmathew on 16.01.17.
  */
object QuillContext extends  JdbcContext[PostgresDialect, SnakeCase](DbConfig.DbPostgresPrefix) {
  DbMigration.migratePostgres()
}