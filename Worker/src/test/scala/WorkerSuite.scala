import WorkerState._
import channel.WorkerToMasterChannel
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{ConnectRequest, Empty, SamplingResponse, connectionStartToConnectionFinishMasterGrpc, samplingPivotToSamplingFinishMasterGrpc, samplingStartToSamplingPivotMasterGrpc, sortPartitionStartToSortPartitionFinishMasterGrpc}

import java.io.File
import java.net.InetAddress
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class WorkerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Initiate Connection test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[connectionStartToConnectionFinishMasterGrpc.connectionStartToConnectionFinishMaster]

        when(mockService.connectRequestWorkerToMaster(any(classOf[ConnectRequest])))
            .thenReturn(Future.successful(new Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(connectionStartToConnectionFinishMasterGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Worker.WORKER_STATE = CONNECTION_START

        val request = new Request_WorkerConnection(channel)
        request.initiateConnection()

        verify(mockService, times(1))
            .connectRequestWorkerToMaster(ArgumentMatchers.eq(ConnectRequest(InetAddress.getLocalHost().getHostAddress().toString, 8000)))

        server.shutdown()
        channel.shutdown()
        Thread.sleep(500)
    }

    test("Send sampling result to master") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[samplingStartToSamplingPivotMasterGrpc.samplingStartToSamplingPivotMaster]

        when(mockService.samplingResult(any(classOf[SamplingResponse])))
            .thenReturn(Future.successful(new Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(samplingStartToSamplingPivotMasterGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        WorkerArgumentHandler.inputFileArray = Array(new File("Worker//data//sortA1"))
        Worker.WORKER_STATE = SAMPLING_SAMPLE
        WorkerToMasterChannel.channel = channel

        val request = Request_WorkerSamplingFirst.sendSampledDataToMaster()

        verify(mockService, times(1))
            .samplingResult(ArgumentMatchers.eq(SamplingResponse(
                Request_WorkerSamplingFirst.sampleFromFile(new File("Worker//data//sortA1")))))

        server.shutdown()
        channel.shutdown()
        Thread.sleep(500)
    }

    test("Send sampling is finished to master") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[samplingPivotToSamplingFinishMasterGrpc.samplingPivotToSamplingFinishMaster]

        when(mockService.samplePartitionFinished(any(classOf[Empty])))
            .thenReturn(Future.successful(new Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(samplingPivotToSamplingFinishMasterGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Worker.WORKER_STATE = SAMPLING_FINISH
        WorkerToMasterChannel.channel = channel

        val request = Request_WorkerSamplingSecond.sendSampledDataToMaster()

        verify(mockService, times(1))
            .samplePartitionFinished(ArgumentMatchers.eq(Empty()))

        server.shutdown()
        channel.shutdown()
        Thread.sleep(500)
    }

    test("Send sorting is finished to master") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[sortPartitionStartToSortPartitionFinishMasterGrpc.sortPartitionStartToSortPartitionFinishMaster]

        when(mockService.finishedSorting(any(classOf[Empty])))
            .thenReturn(Future.successful(new Empty()))

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(sortPartitionStartToSortPartitionFinishMasterGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        Worker.WORKER_STATE = SORT_PARTITION_FINISH
        WorkerToMasterChannel.channel = channel

        Request_WorkerSort.sendSortFinished()

        verify(mockService, times(1))
            .finishedSorting(ArgumentMatchers.eq(Empty()))

        server.shutdown()
        channel.shutdown()
        Thread.sleep(500)
    }
}
