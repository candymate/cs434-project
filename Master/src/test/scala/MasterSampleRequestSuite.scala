import MasterState._
import config.ClientInfo
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{Empty, SamplingRequest, SamplingResponse, SortingRequest, SortingResponse, restPhaseServiceGrpc, sampleWorkerServiceGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterSampleRequestSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("sampling request test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[sampleWorkerServiceGrpc.sampleWorkerService]

        when(mockService.masterToWorkerSampleRequest(any(classOf[SamplingRequest]))).thenReturn(
            Future(Empty())
        )

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(sampleWorkerServiceGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        val channelArray = Array(channel)

        Master.MASTER_STATE = SAMPLING_START

        val masterServer = new MasterSampleStartRequest(channelArray)
        masterServer.broadcastSampleStart()

        verify(mockService, times(1))
            .masterToWorkerSampleRequest(ArgumentMatchers.eq(SamplingRequest()))

        server.shutdown()

        Thread.sleep(500)
    }

    /*test("sorting request test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[restPhaseServiceGrpc.restPhaseService]

        when(mockService.sort(any(classOf[SortingRequest]))).thenReturn(
            Future.successful(SortingResponse(true))
        )

        val server: Server = InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(restPhaseServiceGrpc.bindService(mockService, executorContext))
            .build()
            .start()

        val channel = InProcessChannelBuilder
            .forName(serverName)
            .directExecutor()
            .build()

        val channelArray = Array(channel)
        val clientInfoMap = mutable.Map[Int, ClientInfo]()

        clientInfoMap.put(1, new ClientInfo("localhost", 8000))

        val masterServer = new MasterSampleSortRequest(clientInfoMap, channelArray, List("ABCEDFAS"))
        masterServer.sendSortRequestToEveryClient()

        verify(mockService, times(1))
            .sort(ArgumentMatchers.eq(SortingRequest(
                List("ABCEDFAS")
            )))

        server.shutdown()

        Thread.sleep(500)
    }*/
}
