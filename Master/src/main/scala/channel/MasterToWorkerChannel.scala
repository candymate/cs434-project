package channel

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.slf4j.LoggerFactory
import protobuf.connect.Empty

object MasterToWorkerChannel {
    val log = LoggerFactory.getLogger(getClass)
    var ipList: Array[String] = Array()
    var portList: Array[Int] = Array()
    var channelList: Array[ManagedChannel] = Array()

    def configureClientIpAndPort(ip: String, port: Int) = {
        require(ip != null)

        this.ipList :+ ip
        this.portList :+ port
    }

    def openMasterToWorkerChannelArray() = {
        assert(ipList.size == portList.size)

        (ipList, portList).zipped.toList foreach {
            x => {
                val managedChannelBuilder = ManagedChannelBuilder.forAddress(x._1, x._2)
                managedChannelBuilder.usePlaintext()
                val channel = managedChannelBuilder.build()
                channelList :+ channel
            }
        }
    }

    def closeMasterToWorkerChannelArray() = {
        channelList foreach {
            x => x.shutdown()
        }
    }

    def sendMessageToEveryClient(sendMessage: (ManagedChannel => Any)): Unit = {
        channelList foreach {
            sendMessage
        }
    }

}