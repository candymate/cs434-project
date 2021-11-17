import config.MasterConfig
import io.grpc.Server
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectRequest, Empty}
import protobuf.connect.connectServiceGrpc.connectService

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    private class ServerWorkerConnectionMock {
        val server: Server = null
        def start(): Unit = {
            server.start()
            server.awaitTermination()
        }
        def shutdown(): Unit = {
            server.shutdown()
        }

        private class connect extends connectService {
            override def connect(request: ConnectRequest): Future[Empty] = {
                Future.successful(new Empty())
            }
        }
    }



    test("Worker connect request test") {
        val connectionService = new ServerWorkerConnectionMock()
        val connection = new WorkerConnection(new MasterConfig("localhost", 9000))
    }
}
