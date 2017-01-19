package org.dcs.nifi.repository

import io.getquill.{CassandraSyncContext, SnakeCase}

/**
  * Created by cmathew on 16.01.17.
  */
object QuillContext extends CassandraSyncContext[SnakeCase]("cassandra")
