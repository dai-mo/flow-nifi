package org.dcs.nifi.repository

import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.dcs.nifi.services.ServicesUnitSpec
import org.scalatest.Ignore

/**
  * Created by cmathew on 08.12.16.
  */
@Ignore // set to ignore until integration environment is setup
class ContentRespositorySpec extends ServicesUnitSpec {

  "Content Repository" should "be valid for every stage in processor content lifecycle" in {

    val data = "Sample Flow Data Content".getBytes(StandardCharsets.UTF_8)

    val cr = new CassandraContentRepository()
    val claim = cr.create(true)
    assert(claim.getLength == -1)

    var fdc = cr.getContentRecord(claim)
    assert(fdc.isDefined)
    assert(fdc.get.claimCount == 0)
    assert(fdc.get.data.isEmpty)


    val os = cr.write(claim)
    os.write(data)
    os.flush()
    os.close()
    assert(claim.getLength == data.length)

    fdc = cr.getContentRecord(claim)
    assert(fdc.isDefined)
    assert(fdc.get.claimCount == 0)

    assert(fdc.get.data.deep == data.deep)

    val is = cr.read(claim)
    val isData = IOUtils.toByteArray(is)
    is.close()
    assert(isData.deep == data.deep)

    assert(cr.size(claim) == data.length)
    assert(cr.isAccessible(claim))

    assert(cr.incrementClaimaintCount(claim) == 1)
    fdc = cr.getContentRecord(claim)
    assert(fdc.isDefined)
    assert(fdc.get.claimCount == 1)
    assert(cr.getClaimantCount(claim) == 1)

    assert(cr.decrementClaimantCount(claim) == 0)
    fdc = cr.getContentRecord(claim)
    assert(fdc.isDefined)
    assert(fdc.get.claimCount == 0)

    val claimClone = cr.clone(claim, true)
    assert(claimClone.getLength == data.length)
    assert(claimClone.getResourceClaim.getId != claim.getResourceClaim.getId)
    assert(claimClone.getLength == claim.getLength)

    val fdcClone = cr.getContentRecord(claimClone)
    assert(fdcClone.isDefined)
    assert(fdcClone.get.data.deep == fdc.get.data.deep)

    assert(cr.remove(claimClone))
    assert(cr.getContentRecord(claimClone).isEmpty)

    cr.purge()

    cr.shutdown()

    intercept[Exception] {
      cr.create(true)
    }
  }
}
