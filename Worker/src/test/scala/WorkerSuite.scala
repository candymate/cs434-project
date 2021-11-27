import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectRequest, Empty}

import java.net.InetAddress
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Client unit test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[connectMasterServiceGrpc.connectMasterService]

        when(mockService.workerToMasterConnect(any(classOf[ConnectRequest]))).thenReturn(Future.successful(Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(connectMasterServiceGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        val workerConnection = new Request_WorkerConnection(channel)
        workerConnection.initiateConnection()

        verify(mockService, times(1))
            .workerToMasterConnect(ArgumentMatchers.eq(ConnectRequest(InetAddress.getLocalHost().getHostAddress(), 8000)))

        server.shutdown()

        Thread.sleep(500)
    }
}
