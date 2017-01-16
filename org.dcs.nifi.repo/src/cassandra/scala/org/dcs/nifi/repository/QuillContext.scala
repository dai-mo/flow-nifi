package org.dcs.nifi.repository

import io.getquill.{CassandraSyncContext, SnakeCase}

/**
  * Created by cmathew on 16.01.17.
  */
class QuillContext extends CassandraSyncContext[SnakeCase]("cassandra")
