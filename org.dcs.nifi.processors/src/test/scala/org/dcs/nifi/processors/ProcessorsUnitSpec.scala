package org.dcs.nifi.processors

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
  with MockitoSugar

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

