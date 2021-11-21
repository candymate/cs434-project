import config.WorkerServerConfig
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect
import protobuf.connect.{SamplingRequest, SamplingResponse, restPhaseServiceGrpc}

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class WorkerServer (val inputPathFileList: Array[File]) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    private def start(): Unit = {
        val serverBuilder = ServerBuilder.forPort(WorkerServerConfig.port)

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

    private def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    private class restPhaseService extends restPhaseServiceGrpc.restPhaseService {
        override def sample(request: SamplingRequest): Future[SamplingResponse] = {
            log.info("sample request message received... start sampling")
            Future {
                new connect.SamplingResponse(WorkerSampling.sampleFromFile(inputPathFileList(0)))
            }
        }
    }

    start()
    blockUntilShutdown()
}
