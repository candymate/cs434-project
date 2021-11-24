import config.{ClientInfo, MasterConfig}
import io.grpc.ManagedChannelBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.io.File
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class MainSuite extends AnyFunSuite {
    implicit val threadPool: ExecutorService = Executors.newFixedThreadPool(8)
    implicit val executorContext: ExecutionContext = ExecutionContext.fromExecutorService(threadPool)

    test("Connection test") {
        val numberOfConnection = 5
        val clientConnection: List[Future[Unit]] = Nil

        val openServer = Future {
            val testMaster = new MasterConnection(numberOfConnection, executorContext)

            assert(testMaster != null)

            Thread.sleep(500)

            assert(testMaster.server.isTerminated)
        }

        for(
            i <- 0 until numberOfConnection
        ) yield {
            clientConnection :+ Future {
                val testClient = new WorkerConnection(new MasterConfig("127.0.0.1", 9000), null)

                testClient.connect()
            }
        }

        Await.result(openServer, Duration.Inf)
        clientConnection foreach(x =>
            Await.result(x, Duration.Inf)
        )
    }

    test("Sampling phase test") {
        val openServer = Future {
            val testFile = new File("Worker//data//partition1")
            val openSamplingServer = new WorkerServer(Array(testFile), null, executorContext)

            Thread.sleep(500)
        }

        val managedChannelBuilder = ManagedChannelBuilder.forAddress("localhost", 8000)
        managedChannelBuilder.usePlaintext()
        val channel = managedChannelBuilder.build()

        val clientInfoMap = mutable.Map[Int, ClientInfo]()

        val masterClient = new MasterSampleSortRequest(clientInfoMap, Array(channel), null)
        masterClient.sendSampleRequestToEveryClient()

        assertResult(3000) (masterClient.sampledData.size)

        assert("AsfAGHM5om".equals(masterClient.sampledData(0)))
        assert("Ga]QGzP2q)".equals(masterClient.sampledData(2999)))
    }
}
