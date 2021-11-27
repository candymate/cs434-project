import Master.MASTER_STATE
import MasterState._
import channel.MasterToWorkerChannel
import config.ClientInfo
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{ConnectRequest, Empty, SamplingResponse, connectMasterServiceGrpc, sampleMasterServiceGrpc}

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterSample {
    var numberOfClientsThatSampled: Int = 0
    var sampledRecords: List[String] = Nil

    class Service_MasterSample extends sampleMasterServiceGrpc.sampleMasterService {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def workerToMasterSampleResponse(response: SamplingResponse): Future[Empty] = synchronized {
            assert(MASTER_STATE == SAMPLING_START)

            lock.lock()
            try {
                sampledRecords = sampledRecords ++ response.sampledData
                numberOfClientsThatSampled += 1

                if (numberOfClientsThatSampled == Master.numOfRequiredConnections) {
                    MASTER_STATE = SAMPLING_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}