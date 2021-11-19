import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class MainSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Connection test") {
        assert(1 == 1)
    }
}
