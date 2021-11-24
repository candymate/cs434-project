import config.ClientInfo
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.slf4j.LoggerFactory
import protobuf.connect.{SamplingRequest, SortingRequest}
import protobuf.connect.restPhaseServiceGrpc.{blockingStub, restPhaseServiceBlockingStub}

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class MasterSampleSortRequest(val clientInfoMap: mutable.Map[Int, ClientInfo],
                              channelListParam: Array[ManagedChannel],
                              pivotInfo: List[Int]) {
    val logger = LoggerFactory.getLogger(getClass)
    var channelClientList: Array[ManagedChannel] = channelListParam
    var blockingStubClientList: Array[restPhaseServiceBlockingStub] = Array()

    var sampledData: List[String] = Nil
    val lock = new ReentrantLock()

    if (channelListParam == null) {
        channelClientList = Array()
        clientInfoMap foreach {
            case (k: Int, v: ClientInfo) => {
                val managedChannelBuilder = ManagedChannelBuilder.forAddress(v.ip, v.port)
                managedChannelBuilder.usePlaintext()
                val channel = managedChannelBuilder.build()
                channelClientList = channelClientList :+ channel
            }
        }
    }

    channelClientList foreach {
        channel => blockingStubClientList = blockingStubClientList :+ blockingStub(channel)
    }

    def shutdown(): Unit = {
        channelClientList.foreach(
            x => x.shutdown().awaitTermination(1, TimeUnit.SECONDS)
        )
    }

    def sendSampleRequestToEveryClient(): Unit = {
        logger.info("Sending sample request to every client")
        val request = new SamplingRequest(clientInfoMap.map{ case(k, v) => k -> v.ip}.toMap)
        blockingStubClientList.foreach( x => sampleRequest(request, x) )
        logger.info("Successfully sent sample request to every client")
    }

    def sampleRequest(samplingRequest: SamplingRequest, blockingStub: restPhaseServiceBlockingStub): Unit = {
        try {
            val samplingResponse = blockingStub.sample(samplingRequest)
            lock.lock()
            try {
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

    def sendSortRequestToEveryClient(): Unit = {
        logger.info("Sending sample request to every client")
        val request = new SortingRequest(clientInfoMap.map { case(k, v) => k -> pivotInfo(k - 1) }.toMap)
        blockingStubClientList.foreach( x => sortRequest(request, x))
        logger.info("Successfully sent sample request to every client")
    }

    def sortRequest(sortingRequest: SortingRequest, blockingStub: restPhaseServiceBlockingStub): Unit = {
        try {
            val sortingResponse = blockingStub.sort(sortingRequest)
        } catch {
            case e: StatusRuntimeException => {
                sys.exit(1)
            }
        }
    }
}
