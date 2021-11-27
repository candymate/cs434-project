import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToWorkerChannel
import config.{ClientInfo, WorkerServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect
import protobuf.connect.{ConnectResponse, Empty, SamplingRequest, SamplingResponse, ShufflingRequest, ShufflingResponse, SortingRequest, SortingResponse, connectWorkerServiceGrpc, sampleWorkerServiceGrpc, sortWorkerServiceGrpc}

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IterableHasAsJava

class WorkerServer (executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null
    var clientInfo: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()

    def start(): Unit = {
        assert (WORKER_STATE == CONNECTION_START)

        val serverBuilder = ServerBuilder.forPort(WorkerServerConfig.port)
        serverBuilder.addService(connectWorkerServiceGrpc.bindService(new Service_WorkerConnection, executionContext))
        serverBuilder.addService(sampleWorkerServiceGrpc.bindService(new Service_WorkerSample, executionContext))
        serverBuilder.addService(sortWorkerServiceGrpc.bindService(new Service_WorkerSort, executionContext))
        server = serverBuilder.build().start()

        log.info("Worker Server started, listening on " + WorkerServerConfig.port)
        sys.addShutdownHook {
            System.err.println("*** shutting down gRPC server since JVM is shutting down")
            self.stop()
            System.err.println("*** server shut down")
        }
    }

    def stop(): Unit = {
        if (server != null) {
            server.shutdown()
        }
    }

    def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }
}
