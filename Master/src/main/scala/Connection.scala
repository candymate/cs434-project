import config.MasterServerConfig
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, connectServiceGrpc}

import scala.concurrent.{ExecutionContext, Future}

class Connection(numberOfRequiredConnections: Int) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    private[this] var server: Server = null

    private def start(): Unit = {
        server = ServerBuilder.forPort(MasterServerConfig.port)
            .addService(connectServiceGrpc.bindService(new connectService, ExecutionContext.global))
            .build.start
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
            Future.successful(new Empty())
        }
    }
}
