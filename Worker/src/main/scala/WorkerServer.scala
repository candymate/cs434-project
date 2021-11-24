import config.{ClientInfo, WorkerServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect
import protobuf.connect.{SamplingRequest, SamplingResponse, ShufflingRequest, ShufflingResponse, SortingRequest, SortingResponse, restPhaseServiceGrpc}

import java.io.File
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IterableHasAsJava

class WorkerServer (val inputPathFileList: Array[File],
                    val outputPathFile: File,
                    executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null
    var clientInfo: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()

    private def start(): Unit = {
        val serverBuilder = ServerBuilder.forPort(WorkerServerConfig.port)
        serverBuilder.addService(restPhaseServiceGrpc.bindService(new restPhaseService, executionContext))
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

    private def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    private class restPhaseService extends restPhaseServiceGrpc.restPhaseService {
        private val lock = new ReentrantLock()

        override def sample(request: SamplingRequest): Future[SamplingResponse] = {
            log.info("sample request message received")
            lock.lock()
            try {
                log.info("processing other client info from sampling request")
                val requestIPMap = request.ipList
                requestIPMap.foreach {
                    case (k: Int, v: String) => {
                        clientInfo.put(k, new ClientInfo(v, 8000))
                    }
                }
            } finally {
                lock.unlock()
            }

            log.info("start sampling")
            Future {
                connect.SamplingResponse(sampledData = WorkerSampling.sampleFromFile(inputPathFileList(0)))
            }
        }

        override def sort(request: SortingRequest): Future[SortingResponse] = {
            WorkerSortAndPartition.sortAndPartitionFromInputFileList(inputPathFileList, outputPathFile,
                request.pivot.toList)
            Future.successful(
                SortingResponse()
            )
        }

        override def shuffleStart(request: ShufflingRequest): Future[ShufflingResponse] = ???
    }

    start()
    blockUntilShutdown()
}
