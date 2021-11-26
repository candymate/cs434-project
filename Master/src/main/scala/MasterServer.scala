import Master.MASTER_STATE
import MasterState._
import config.MasterServerConfig
import io.grpc.{ManagedChannel, Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import protobuf.connect.connectMasterServiceGrpc

class MasterServer(executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    def start(): Unit = {
        assert(Master.MASTER_STATE == CONNECTION_START)
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)
        serverBuilder.addService(connectMasterServiceGrpc.bindService(new MasterConnectionService, executionContext))
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
}
