import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToWorkerChannel
import config.{ClientInfo, WorkerServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect
import protobuf.connect.{ConnectResponse, Empty, SamplingRequest, SamplingResponse, ShufflingRequest, ShufflingResponse, SortingRequest, SortingResponse, connectWorkerServiceGrpc, restPhaseServiceGrpc, sampleWorkerServiceGrpc}

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

    def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    private class connectWorkerService extends connectWorkerServiceGrpc.connectWorkerService {
        private val lock = new ReentrantLock()

        override def masterToWorkerConnectResponse(connectResponse: ConnectResponse): Future[Empty] = {
            assert(WORKER_STATE == CONNECTION_START)

            log.info("Master to Worker response connect response received")
            lock.lock()
            try {
                WorkerToWorkerChannel.ipList = connectResponse.ipList.toArray
                WorkerToWorkerChannel.portList = connectResponse.portList.toArray
                WORKER_STATE = SAMPLING_START
                Future.successful {
                    Empty()
                }
            } finally {
                lock.unlock()
            }
        }
    }

    private class restPhaseService extends restPhaseServiceGrpc.restPhaseService {
        private val lock = new ReentrantLock()

        override def sample(request: SamplingRequest): Future[SamplingResponse] = ??? /*{
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
        }*/

        override def sort(request: SortingRequest): Future[SortingResponse] = ??? /* {
            WorkerSortAndPartition.sortAndPartitionFromInputFileList(inputPathFileList, outputPathFile,
                request.pivot.toList)
            Future.successful(
                SortingResponse()
            )
        }*/

        override def shuffleStart(request: ShufflingRequest): Future[ShufflingResponse] = ???
    }
}
