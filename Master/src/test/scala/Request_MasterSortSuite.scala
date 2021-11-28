import MasterState._
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{Empty, sortPartitionStartToSortPartitionFinishWorkerGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterSortSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Test sort phase initiation") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[sortPartitionStartToSortPartitionFinishWorkerGrpc.sortPartitionStartToSortPartitionFinishWorker]

        when(mockService.startSorting(any(classOf[Empty]))).thenReturn(Future.successful(Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(sortPartitionStartToSortPartitionFinishWorkerGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Master.MASTER_STATE = SORT_PARTITION_START

        val channelArray = Array(channel)

        val request = new Request_MasterSort(channelArray)
        request.broadcastSortStart()

        verify(mockService, times(1))
            .startSorting(ArgumentMatchers.eq(Empty()))

        server.shutdown()
    }
}
