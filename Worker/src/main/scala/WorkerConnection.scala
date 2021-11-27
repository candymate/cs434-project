import Worker.WORKER_STATE
import WorkerState._
import channel.WorkerToMasterChannel
import io.grpc.{ManagedChannel, StatusRuntimeException}
import org.slf4j.LoggerFactory
import protobuf.connect.{ConnectRequest, connectMasterServiceGrpc}

import java.net.InetAddress

class WorkerConnection(channelParam: ManagedChannel) {
    val logger = LoggerFactory.getLogger(getClass)
    var channel = channelParam

    if (channel == null) {
        WorkerToMasterChannel.openWorkerToMasterChannel()
        channel = WorkerToMasterChannel.channel
    }

    val blockingStub = connectMasterServiceGrpc.blockingStub(channel)

    def initiateConnection(): Unit = {
        assert(WORKER_STATE == CONNECTION_START)

        logger.info(s"trying to connect to master ${WorkerToMasterChannel.ip}:${WorkerToMasterChannel.port} ")
        val request = new ConnectRequest(InetAddress.getLocalHost().getHostAddress(),
            port = WorkerArgumentHandler.optionalWorkerServerPort)

        try {
            logger.info("connection request sent")
            val response = blockingStub.workerToMasterConnect(request)
            WORKER_STATE = CONNECTION_FINISH
        } catch {
            case e: StatusRuntimeException => {
                logger.error("connection rpc failed")
                sys.exit(1)
            }
        }
    }

}
