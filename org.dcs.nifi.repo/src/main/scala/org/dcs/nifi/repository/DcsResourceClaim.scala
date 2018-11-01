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

import java.time.Instant
import java.util.{Date, UUID}

import org.apache.nifi.controller.repository.claim.ResourceClaim

/**
  * Created by cmathew on 08.12.16.
  */
class DcsResourceClaim(lossTolerant: Boolean) extends ResourceClaim {

  val createTimestamp = Date.from(Instant.now())
  val uuid = UUID.randomUUID()

  override def getSection: String = ""

  override def getId: String = uuid.toString

  override def isLossTolerant: Boolean = lossTolerant

  override def getContainer: String = ""

  override def compareTo(rc: ResourceClaim): Int = {
    if(rc == null || !rc.isInstanceOf[DcsResourceClaim])
      1
    else {
      val that = rc.asInstanceOf[DcsResourceClaim]
      this.createTimestamp.compareTo(that.createTimestamp)
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[DcsResourceClaim])
      false
    else {
      val that = obj.asInstanceOf[DcsResourceClaim]
      this.uuid == that.uuid && this.createTimestamp.eq(that.createTimestamp)
    }
  }

  override def hashCode(): Int = uuid.hashCode

  override def toString: String = "ResourceClaim[id=" + uuid.toString + ", createTimestamp=" + createTimestamp.toString + "]"
}
