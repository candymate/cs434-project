import MasterState._
import config.{ClientInfo, MasterServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, connectServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class MasterConnection(numberOfRequiredConnections: Int, executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null
    // key: machine order, value: ClientInfo
    var clientInfoMap: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()

    def start(): Unit = {
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)
        serverBuilder.addService(connectServiceGrpc.bindService(new connectService, executionContext))
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

    private class connectService extends connectServiceGrpc.connectService {
        private val lock = new ReentrantLock()

        override def connect(request: ConnectRequest): Future[Empty] = {
            log.info("connection established with " + request.ip + ":" + 9000)

            lock.lock()
            try {
                clientInfoMap.put(clientInfoMap.size, new ClientInfo(request.ip, 8000))
                if (numberOfRequiredConnections == clientInfoMap.size) {
                    log.info(s"Master successfully connected to ${numberOfRequiredConnections} client(s)")

                    println(s"${InetAddress.getLocalHost().getHostAddress()}:9000")
                    clientInfoMap foreach {case(_, v: ClientInfo) => print(s"${v.ip} ")}
                    println()

                    Master.MASTER_STATE = CONNECTION_FINISH
                    notify()
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}
