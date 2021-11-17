import config.MasterConfig
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.slf4j.LoggerFactory
import protobuf.connect.{ConnectRequest, connectServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.TimeUnit

class Connection(masterConfig: MasterConfig) {
    val logger = LoggerFactory.getLogger(getClass)
    // will change
    val managedChannelBuilder = ManagedChannelBuilder.forAddress(masterConfig.ip, masterConfig.port)
    managedChannelBuilder.usePlaintext()

    val channel = managedChannelBuilder.build()
    val blockingStub = connectServiceGrpc.blockingStub(channel)

    def shutdown(): Unit = {
        logger.info("worker shutdown")
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

    def connect(): Unit = {
        logger.info(s"trying to connect to master ${masterConfig.ip}:${masterConfig.port} ")
        val request = new ConnectRequest(InetAddress.getLocalHost().getHostAddress())

        try {
            logger.info("connection request sent")
            val response = blockingStub.connect(request)
        } catch {
            case e: StatusRuntimeException => {
                logger.error("connection rpc failed")
                sys.exit(1)
            }
        }
    }

    connect()
}
