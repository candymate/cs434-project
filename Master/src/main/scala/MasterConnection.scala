import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import config.{ClientInfo, MasterServerConfig}
import io.grpc.{ManagedChannel, Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, ConnectResponse, Empty, connectMasterServiceGrpc, connectWorkerServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.{ExecutionContext, Future}

class MasterConnection(numberOfRequiredConnections: Int, executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    def start(): Unit = {
        assert(Master.MASTER_STATE == CONNECTION_START)
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)
        serverBuilder.addService(connectMasterServiceGrpc.bindService(new connectService, executionContext))
        server = serverBuilder.build().start()
        log.info("Server started, listening on " + MasterServerConfig.port)
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

    def broadcastConnectionIsFinished() = {
        assert (MASTER_STATE == CONNECTION_FINISH)

        def broadcastConnectionResponse(x: ManagedChannel) = {
            val blockingStub = connectWorkerServiceGrpc.blockingStub(x)
            blockingStub.masterToWorkerConnectResponse(new ConnectResponse(
                MasterToWorkerChannel.ipList, MasterToWorkerChannel.portList))
        }

        MasterToWorkerChannel.openMasterToWorkerChannelArray()
        MasterToWorkerChannel.sendMessageToEveryClient(broadcastConnectionResponse)
        MasterToWorkerChannel.closeMasterToWorkerChannelArray()

        MASTER_STATE = SAMPLING_START
    }

    private class connectService extends connectMasterServiceGrpc.connectMasterService {
        private val lock = new ReentrantLock()

        override def workerToMasterConnect(request: ConnectRequest): Future[Empty] = synchronized {
            log.info("connection established with " + request.ip + ":" + request.port)

            lock.lock()
            try {
                Master.clientInfoMap.put(Master.clientInfoMap.size, new ClientInfo(request.ip, request.port))
                if (numberOfRequiredConnections == Master.clientInfoMap.size) {
                    log.info(s"Master successfully connected to ${numberOfRequiredConnections} client(s)")

                    MasterToWorkerChannel.configureClientIpAndPort(request.ip, request.port)

                    println(s"${InetAddress.getLocalHost().getHostAddress()}:9000")
                    Master.clientInfoMap foreach { case (_, v: ClientInfo) => print(s"${v.ip} ") }
                    println()

                    MASTER_STATE = CONNECTION_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}
