import config.ClientInfo
import io.grpc.Server
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{SamplingRequest, SamplingResponse, restPhaseServiceGrpc}

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MasterSampleSortRequestSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("sampling request test") {
        val serverName = InProcessServerBuilder.generateName()
        val mockService = mock[restPhaseServiceGrpc.restPhaseService]

        when(mockService.sample(any(classOf[SamplingRequest]))).thenReturn(
            Future.successful(SamplingResponse(List("Apple", "Banana", "Cat", "Dent")))
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

        val masterServer = new MasterSampleSortRequest(clientInfoMap, channelArray, null)
        masterServer.sendSampleRequestToEveryClient()

        verify(mockService, times(1))
            .sample(ArgumentMatchers.eq(SamplingRequest(
                clientInfoMap.map{ case(k, v) => k -> v. ip}.toMap)))

        server.shutdown()

        Thread.sleep(500)
    }
}
