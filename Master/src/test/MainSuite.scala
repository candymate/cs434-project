package streams

import org.scalatest.funsuite.{AnyFunSuite => FunSuite}

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MainSuite extends FunSuite {
    test("Master project test") {
        assert(1 == 1)
    }
}
