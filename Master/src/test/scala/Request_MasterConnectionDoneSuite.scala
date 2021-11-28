import MasterState._
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectResponse, Empty, connectionStartToConnectionFinishWorkerGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class Request_MasterConnectionDoneSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("client info broadcast test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[connectionStartToConnectionFinishWorkerGrpc.connectionStartToConnectionFinishWorker]

        when(mockService.broadCastClientInfo(any(classOf[ConnectResponse]))).thenReturn(
            Future.successful(Empty())
        )

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(connectionStartToConnectionFinishWorkerGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Master.MASTER_STATE = CONNECTION_FINISH

        val channelArray = Array(channel)

        val request = new Request_MasterConnectionDone(channelArray)
        request.broadcastConnectionDone()

        verify(mockService, times(1))
            .broadCastClientInfo(ConnectResponse(ipList = Seq("localhost", "localhost", "localhost"), portList = Seq(0, 0, 0)))

        server.shutdown()
    }

}
