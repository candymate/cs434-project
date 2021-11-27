/*
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectRequest, connectMasterServiceGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterServerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    private class MasterServerMock() {
        val managedChannelBuilder = ManagedChannelBuilder.forAddress("localhost", 9000)
        managedChannelBuilder.usePlaintext()
        val channel = managedChannelBuilder.build()
        val blockingStub = connectMasterServiceGrpc.blockingStub(channel)

        def connect(): Unit = {
            val request = new ConnectRequest("localhost")
            try {
                val response = blockingStub.workerToMasterConnect(request)
            } catch {
                case e: StatusRuntimeException => {
                    sys.exit(1)
                }
            }
        }
    }

    test("server connects from client") {
       val openServer = Future {
           Master.numOfRequiredConnections = 3
            val testConnection = new MasterServer(ExecutionContext.global)
            testConnection.start()

           Thread.sleep(2000)

            assert(testConnection.server != null)
            assertResult(3)(Master.clientInfoMap.size)

            Thread.sleep(5000)

            testConnection.stop()
        }

        val mockClient1 = new MasterServerMock()
        val mockClient2 = new MasterServerMock()
        val mockClient3 = new MasterServerMock()

        Thread.sleep(100)

        mockClient1.connect()
        mockClient2.connect()
        mockClient3.connect()

        Await.result(openServer, Duration.Inf)
    }
}
*/
