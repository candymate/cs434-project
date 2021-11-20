import config.MasterConfig
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.mockito.AdditionalAnswers.delegatesTo
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.IdiomaticMockito.StubbingOps
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect
import protobuf.connect.{ConnectRequest, Empty, connectServiceGrpc}
import protobuf.connect.connectServiceGrpc.{bindService, connectService}

import java.net.InetAddress
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Client unit test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[connectServiceGrpc.connectService]

        when(mockService.connect(any(classOf[ConnectRequest]))).thenReturn(Future.successful(Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(connectServiceGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        new WorkerConnection(new MasterConfig("localhost", 9000), channel)

        verify(mockService, times(1))
            .connect(ArgumentMatchers.eq(ConnectRequest(InetAddress.getLocalHost().getHostAddress())))

        server.shutdown()

        Thread.sleep(500)
    }
}
