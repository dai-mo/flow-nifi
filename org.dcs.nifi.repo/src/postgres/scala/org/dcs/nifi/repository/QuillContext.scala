package org.dcs.nifi.repository

import io.getquill.{JdbcContext, PostgresDialect, SnakeCase}

/**
  * Created by cmathew on 16.01.17.
  */
class QuillContext extends  JdbcContext[PostgresDialect, SnakeCase]("postgres")