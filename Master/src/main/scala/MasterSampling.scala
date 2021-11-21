import config.ClientInfo
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.slf4j.LoggerFactory
import protobuf.connect.SamplingRequest
import protobuf.connect.restPhaseServiceGrpc.{blockingStub, restPhaseServiceBlockingStub}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class MasterSampling(val clientInfoMap: mutable.Map[Int, ClientInfo],
                     channelListParam: Array[ManagedChannel],
                     val numberOfConnection: Int) {
    val logger = LoggerFactory.getLogger(getClass)
    var channelClientList: Array[ManagedChannel] = channelListParam
    var blockingStubClientList: Array[restPhaseServiceBlockingStub] = Array()

    var currentConnection: Int = 0
    var sampledData: List[String] = Nil
    val lock = new ReentrantLock()

    if (channelListParam == null) {
        channelClientList = Array()
        clientInfoMap foreach {
            case (k: Int, v: ClientInfo) => {
                val managedChannelBuilder = ManagedChannelBuilder.forAddress(v.ip, v.port)
                managedChannelBuilder.usePlaintext()
                val channel = managedChannelBuilder.build()
                channelClientList :+ channel
                blockingStubClientList :+ blockingStub(channel)
            }
        }
    }

    def shutdown(): Unit = {
        channelClientList.foreach(
            x => x.shutdown().awaitTermination(1, TimeUnit.SECONDS)
        )
    }

    def sendSampleRequestToEveryClient(): Unit = {
        val request = new SamplingRequest(clientInfoMap.map{ case(k, v) => k -> v.ip}.toMap)
        blockingStubClientList.foreach( x => sampleRequest(request, x) )
    }

    def sampleRequest(samplingRequest: SamplingRequest, blockingStub: restPhaseServiceBlockingStub): Unit = {
        try {
            val samplingResponse = blockingStub.sample(samplingRequest)
            lock.lock()
            try {
                currentConnection = currentConnection + 1
                sampledData = sampledData ++ samplingResponse.sampledData
            } finally {
                lock.unlock()
            }
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }

    sendSampleRequestToEveryClient()
}
