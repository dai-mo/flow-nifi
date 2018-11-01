/*
 * Copyright (c) 2017-2018 brewlabs SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dcs.nifi.repository

import org.apache.nifi.controller.repository.claim.{ContentClaim, ResourceClaim}

/**
  * Created by cmathew on 08.12.16.
  */
class DcsContentClaim(drc: DcsResourceClaim) extends ContentClaim {

  private var length: Long = -1L

  def setLength(length: Long) = this.length = length

  def getTimestamp = drc.createTimestamp

  override def getLength: Long = length

  override def getOffset: Long = 0

  override def getResourceClaim: ResourceClaim = drc

  override def compareTo(o: ContentClaim): Int = drc.compareTo(o.getResourceClaim)

  override def equals(obj: scala.Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[DcsContentClaim])
      false
    else {
      val that = obj.asInstanceOf[DcsContentClaim]
      this.getResourceClaim.eq(that.getResourceClaim)
    }
  }

  override def hashCode(): Int = drc.hashCode()

  override def toString: String = "ContentClaim[rc_id=" + drc.getId + ", createTimestamp=" + drc.createTimestamp.toString + ", length=" + length + "]"
}
