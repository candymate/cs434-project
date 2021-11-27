import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import protobuf.connect.{SamplingRequest, SamplingResponse}

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class WorkerServerSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    /* test ("Sampling response") {
        val testFile = new File("Worker//data//partition1")

        val workerServer = Future {
            val server = new WorkerServer(Array(testFile), null, executorContext)
            Thread.sleep(500)
            server.stop()
        }

        val channelBuilder = ManagedChannelBuilder.forAddress("localhost", 8000)
        channelBuilder.usePlaintext()
        val channel = channelBuilder.build()

        val blockingStub: restPhaseServiceGrpc.restPhaseServiceBlockingStub =
            restPhaseServiceGrpc.blockingStub(channel)

        val clientInfoMock = Map(1 -> "localhost")

        val samplingResponse: SamplingResponse = blockingStub.sample(
            SamplingRequest(clientInfoMock)
        )

        assert("AsfAGHM5om".equals(samplingResponse.sampledData(0)))
        assert("Ga]QGzP2q)".equals(samplingResponse.sampledData(2999)))
        assertResult(3000) (samplingResponse.sampledData.size)

    } */

}
