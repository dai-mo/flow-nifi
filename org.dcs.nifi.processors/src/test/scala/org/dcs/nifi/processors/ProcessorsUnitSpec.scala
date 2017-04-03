package org.dcs.nifi.processors

import org.dcs.api.processor.RemoteProcessor
import org.dcs.remote.RemoteService
import org.mockito.Mockito.{doReturn, spy}
import org.scalatest._
import org.scalatest.junit.JUnitSuite
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar

abstract class ProcessorsBaseUnitSpec extends FlatSpec
  with Matchers
  with OptionValues
  with Inside
  with Inspectors
  with BeforeAndAfterEach
  with BeforeAndAfter
  with MockitoSugar {

  def processorServiceClassName: String

  def mockClientProcessor(remoteProcessor: RemoteProcessor, response: Array[Array[Byte]]): ClientProcessor = {

    object MockRemoteService extends RemoteService with MockZookeeperServiceTracker

    val clientProcessor: WorkerProcessor = spy(new WorkerProcessor())

    doReturn(MockRemoteService).
      when(clientProcessor).
      remoteService

    MockZookeeperServiceTracker.addProcessor(
      processorServiceClassName,
      new MockRemoteProcessorService(remoteProcessor, response)
    )
    clientProcessor
  }
}

// FIXME: Currently the only way to use the mockito
// inject mock mechanism to test the CDI
// part is to run the test as JUnit tests
// since there is no mechanism to run this
// as a scala test.
// ScalaMock could be an option once the 
// issue https://github.com/paulbutcher/ScalaMock/issues/100
// is resolved
abstract class JUnitSpec extends JUnitSuite
  with Matchers
  with OptionValues
  with Inside
  with Inspectors
  with MockitoSugar
  
object IT extends Tag("IT")

