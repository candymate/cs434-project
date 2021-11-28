import MasterState._
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{Empty, PivotResult, samplingSampleToSamplingFinishWorkerGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterSampleSecondSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Test broadcast pivot result") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[samplingSampleToSamplingFinishWorkerGrpc.samplingSampleToSamplingFinishWorker]

        when(mockService.pivotResult(any(classOf[PivotResult]))).thenReturn(Future.successful(Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(samplingSampleToSamplingFinishWorkerGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Master.MASTER_STATE = SAMPLING_PIVOT
        MasterSortSampledRecords.pivotList = List("A", "C")

        val channelArray = Array(channel)

        val request = new Request_MasterSampleSecond(channelArray)
        request.broadcastPivots()

        verify(mockService, times(1))
            .pivotResult(ArgumentMatchers.eq(PivotResult(List("A", "C"))))

        server.shutdown()
    }
}
