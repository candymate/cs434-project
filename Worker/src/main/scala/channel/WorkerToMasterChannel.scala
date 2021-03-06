package channel

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.slf4j.LoggerFactory

object WorkerToMasterChannel {
    val log = LoggerFactory.getLogger(getClass)
    var ip: String = null
    var port: Int = 0
    var channel: ManagedChannel = null

    def configureMasterIpAndPort(ip: String, port: Int) = {
        assert(channel == null && this.ip == null)

        this.ip = ip
        this.port = port
    }

    def openWorkerToMasterChannel() = {
        assert(ip != null)

        val managedChannelBuilder = ManagedChannelBuilder.forAddress(ip, port)
        managedChannelBuilder.usePlaintext()
        channel = managedChannelBuilder.build()
    }

    def closeWorkerToMasterChannel(): Unit = {
        log.info("worker to master channel shutdown")
        channel.shutdown()
    }

}
