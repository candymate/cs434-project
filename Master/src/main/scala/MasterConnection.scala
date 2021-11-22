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

    private def start(): Unit = {
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

    private def stop(): Unit = {
        if (server != null) {
            server.shutdown()
        }
    }

    private def blockUntilShutdown(): Unit = {
        if (server != null) {
            server.awaitTermination()
        }
    }

    // implement service for counting connections
    // also need to keep ip address and port number for later
    // if numberOfRequiredConnections == currentConnections, then stop the server

    private class connectService extends connectServiceGrpc.connectService {
        private val lock = new ReentrantLock()

        override def connect(request: ConnectRequest): Future[Empty] = {
            log.info("connection established with " + request.ip + ":" + 9000)

            lock.lock()
            try {
                clientInfoMap.put(clientInfoMap.size + 1, new ClientInfo(request.ip, 8000))
                if (numberOfRequiredConnections == clientInfoMap.size) {
                    log.info(s"Master successfully connected to ${numberOfRequiredConnections} client(s)")

                    println(s"${InetAddress.getLocalHost().getHostAddress()}:9000")

                    clientInfoMap foreach {case(_, v: ClientInfo) => print(s"${v.ip} ")}

                    server.shutdown()
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }

    log.info(s"started master server expecting ${numberOfRequiredConnections} slave(s)")
    start()
    blockUntilShutdown()
}
