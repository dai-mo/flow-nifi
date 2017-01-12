package org.dcs.nifi.repo

import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitSuite
import org.scalatest.mock.MockitoSugar

/**
  * Created by cmathew on 08.12.16.
  */
abstract class RepoUnitSpec extends FlatSpec
  with Matchers
  with OptionValues
  with Inside
  with Inspectors
  with MockitoSugar
  with ScalaFutures

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
