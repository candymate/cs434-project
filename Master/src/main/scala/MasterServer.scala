import Master.MASTER_STATE
import MasterState._
import Service_MasterSample.Service_MasterSample
import Service_MasterSort.Service_MasterSort
import config.MasterServerConfig
import io.grpc.{ManagedChannel, Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import protobuf.connect.{connectMasterServiceGrpc, sampleMasterServiceGrpc, sortMasterServiceGrpc}

class MasterServer(executionContext: ExecutionContext) { self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    def start(): Unit = {
        assert(Master.MASTER_STATE == CONNECTION_START)
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)

        // add services here
        serverBuilder.addService(connectMasterServiceGrpc.bindService(new Service_MasterConnection, executionContext))
        serverBuilder.addService(sampleMasterServiceGrpc.bindService(new Service_MasterSample, executionContext))
        serverBuilder.addService(sortMasterServiceGrpc.bindService(new Service_MasterSort, executionContext))

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
