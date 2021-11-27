import Worker.WORKER_STATE
import WorkerState._
import config.{ClientInfo, WorkerServerConfig}
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class WorkerServer(executionContext: ExecutionContext) {
    self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null
    var clientInfo: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()

    def start(): Unit = {
        assert(WORKER_STATE == SERVER_START)

        val serverBuilder = ServerBuilder.forPort(WorkerServerConfig.port)

        // add services here


        server = serverBuilder.build().start()

        log.info("Worker Server started, listening on " + WorkerServerConfig.port)
        sys.addShutdownHook {
            System.err.println("*** shutting down gRPC server since JVM is shutting down")
            self.stop()
            System.err.println("*** server shut down")
        }

        WORKER_STATE = SERVER_FINISH
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
