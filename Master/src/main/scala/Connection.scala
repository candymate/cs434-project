import config.{ClientInfo, MasterServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, connectServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}

class Connection(numberOfRequiredConnections: Int, executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    private[this] var server: Server = null
    // key: machine order, value: ClientInfo
    private[this] var clientInfoMap = new ConcurrentHashMap[Int, ClientInfo]()

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
        override def connect(request: ConnectRequest): Future[Empty] = {
            log.info("connection established")
            clientInfoMap.put(clientInfoMap.size() + 1, new ClientInfo(request.ip, 9000))

            if (numberOfRequiredConnections == clientInfoMap.size()) {
                println(InetAddress.getLocalHost().getHostAddress())

                for (
                    i <- 0 until clientInfoMap.size()
                ) yield (println(clientInfoMap.get(i + 1).ip))

                server.shutdown()
            }

            Future.successful(Empty())
        }
    }

    start()
    blockUntilShutdown()
}
