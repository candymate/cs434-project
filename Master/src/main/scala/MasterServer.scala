import Master.MASTER_STATE
import MasterState._
import config.MasterServerConfig
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

class MasterServer(executionContext: ExecutionContext) {
    self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    def start(): Unit = {
        assert(Master.MASTER_STATE == SERVER_START)
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)

        // add services here


        server = serverBuilder.build().start()
        log.info("Server started, listening on " + MasterServerConfig.port)
        sys.addShutdownHook {
            System.err.println("*** shutting down gRPC server since JVM is shutting down")
            self.stop()
            System.err.println("*** server shut down")
        }

        MASTER_STATE = SERVER_FINISH
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
