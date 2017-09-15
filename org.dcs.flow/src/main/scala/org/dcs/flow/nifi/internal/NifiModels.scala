package org.dcs.flow.nifi.internal

import scala.beans.BeanProperty

/**
  * Created by cmathew on 01/08/16.
  */
case class ProcessGroup(@BeanProperty var id: String,
                        @BeanProperty var name: String,
                        @BeanProperty var version: Long) {
  def this() = this("", "", 0)
}

object ProcessGroupHelper {

  val NameIdDelimiter = ";"
  val RootProcessGroupId = "root"
  val DefaultClientId = "root"

  def extractFromName(comments: String): (String, String) = {
    val idName = comments.split(NameIdDelimiter)
    (idName(0), idName(1))
  }
}

