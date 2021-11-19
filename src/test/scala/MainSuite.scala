import config.MasterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MainSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Connection test") {
        val numberOfConnection = 5
        val clientConnection: List[Future[Unit]] = Nil

        val openServer = Future {
            val testMaster = new MasterConnection(numberOfConnection, executorContext)

            assert(testMaster != null)

            Thread.sleep(500)

            assert(testMaster.server.isTerminated)
        }

        for(
            i <- 0 until numberOfConnection
        ) yield {
            clientConnection :+ Future {
                val testClient = new WorkerConnection(new MasterConfig("127.0.0.1", 9000))

                testClient.connect()
            }
        }

        Await.result(openServer, Duration.Inf)
        clientConnection foreach(x =>
            Await.result(x, Duration.Inf)
        )
    }
}
