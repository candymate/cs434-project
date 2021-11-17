import io.grpc.Server
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class ConnectionSuite extends AnyFunSuite {
    test("server start") {
        assert(1 + 1 == 2)
    }
}
