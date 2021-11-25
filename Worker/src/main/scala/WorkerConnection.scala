import WorkerState.CONNECTION_START
import channel.WorkerToMasterChannel
import io.grpc.{ManagedChannel, StatusRuntimeException}
import org.slf4j.LoggerFactory
import protobuf.connect.{ConnectRequest, connectServiceGrpc}

import java.net.InetAddress

class WorkerConnection(channelParam: ManagedChannel) {
    val logger = LoggerFactory.getLogger(getClass)
    var channel = channelParam

    if (channel == null) {
        WorkerToMasterChannel.openWorkerToMasterChannel()
        channel = WorkerToMasterChannel.channel
    }

    val blockingStub = connectServiceGrpc.blockingStub(channel)

    def initiateConnection(): Unit = {
        assert(Worker.WORKER_STATE == CONNECTION_START)

        logger.info(s"trying to connect to master ${WorkerToMasterChannel.ip}:${WorkerToMasterChannel.port} ")
        val request = new ConnectRequest(InetAddress.getLocalHost().getHostAddress())

        try {
            logger.info("connection request sent")
            val response = blockingStub.connect(request)
        } catch {
            case e: StatusRuntimeException => {
                logger.error("connection rpc failed")
                sys.exit(1)
            }
        } finally {
            if (channelParam == null) {
                WorkerToMasterChannel.closeWorkerToMasterChannel()
            }
        }
    }

}
