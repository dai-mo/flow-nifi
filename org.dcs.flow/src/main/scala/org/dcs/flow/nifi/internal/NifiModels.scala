package org.dcs.flow.nifi.internal

import scala.beans.BeanProperty

/**
  * Created by cmathew on 01/08/16.
  */
case class ProcessGroup(@BeanProperty var id: String,
                        @BeanProperty var name: String) {
  def this() = this("", "")
}

