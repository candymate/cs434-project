import MasterState._
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{Empty, connectionStartToConnectionFinishWorkerGrpc, samplingStartToSamplingSampleWorkerGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterSampleFirstSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Test sample initiation") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[samplingStartToSamplingSampleWorkerGrpc.samplingStartToSamplingSampleWorker]

        when(mockService.startSampling(any(classOf[Empty]))).thenReturn(Future.successful(Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(samplingStartToSamplingSampleWorkerGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Master.MASTER_STATE = SAMPLING_START

        val channelArray = Array(channel)

        val request = new Request_MasterSampleFirst(channelArray)
        request.broadcastSampleStart()

        verify(mockService, times(1))
            .startSampling(Empty())

        server.shutdown()
    }
}
