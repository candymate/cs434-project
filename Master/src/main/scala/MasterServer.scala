import Master.MASTER_STATE
import MasterState._
import Service_MasterMerge.Service_MasterMerge
import Service_MasterSampleFirst.Service_MasterSample
import Service_MasterSampleSecond.Service_MasterSampleSecond
import Service_MasterSort.Service_MasterSort
import Service_MasterShuffle.Service_MasterShuffle
import config.MasterServerConfig
import io.grpc.{Server, ServerBuilder}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect._

import scala.concurrent.ExecutionContext

class MasterServer(executionContext: ExecutionContext) {
    self =>
    val log: Logger = LoggerFactory.getLogger(getClass)

    var server: Server = null

    def start(): Unit = {
        assert(Master.MASTER_STATE == SERVER_START)
        val serverBuilder = ServerBuilder.forPort(MasterServerConfig.port)

        // add services here
        serverBuilder.addService(connectionStartToConnectionFinishMasterGrpc.bindService(new Service_MasterConnection, executionContext))
        serverBuilder.addService(samplingStartToSamplingPivotMasterGrpc.bindService(new Service_MasterSample, executionContext))
        serverBuilder.addService(samplingPivotToSamplingFinishMasterGrpc.bindService(new Service_MasterSampleSecond, executionContext))
        serverBuilder.addService(sortPartitionStartToSortPartitionFinishMasterGrpc.bindService(new Service_MasterSort, executionContext))
        serverBuilder.addService(shuffleStartToShuffleFinishMasterGrpc.bindService(new Service_MasterShuffle, executionContext))
        serverBuilder.addService(mergeStartToMergeFinishMasterGrpc.bindService(new Service_MasterMerge, executionContext))

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
