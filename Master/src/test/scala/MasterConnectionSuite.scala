import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectRequest, connectServiceGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterConnectionSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    private class MasterConnectionMock() {
        val managedChannelBuilder = ManagedChannelBuilder.forAddress("localhost", 9000)
        managedChannelBuilder.usePlaintext()
        val channel = managedChannelBuilder.build()
        val blockingStub = connectServiceGrpc.blockingStub(channel)

        def connect(): Unit = {
            val request = new ConnectRequest("localhost")
            try {
                val response = blockingStub.connect(request)
            } catch {
                case e: StatusRuntimeException => {
                    sys.exit(1)
                }
            }
        }
    }

    test("server connects from client") {
        val openServer = Future {
            val testConnection = new MasterConnection(3, ExecutionContext.global)

            assert(testConnection.server != null)
            assertResult(3)(testConnection.clientInfoMap.size)

            Thread.sleep(100)

            assert(testConnection.server.isTerminated)
        }

        val mockClient1 = new MasterConnectionMock()
        val mockClient2 = new MasterConnectionMock()
        val mockClient3 = new MasterConnectionMock()

        Thread.sleep(100)

        mockClient1.connect()
        mockClient2.connect()
        mockClient3.connect()

        Await.result(openServer, Duration.Inf)
    }
}
