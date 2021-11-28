import Master.MASTER_STATE
import MasterState._
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{Empty, SamplingResponse, samplingStartToSamplingPivotMasterGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterSampleFirst {
    var numberOfClientsThatSampled: Int = 0
    var sampledRecords: List[String] = Nil

    class Service_MasterSample extends samplingStartToSamplingPivotMasterGrpc.samplingStartToSamplingPivotMaster {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def samplingResult(response: SamplingResponse): Future[Empty] = synchronized {
            assert(MASTER_STATE == SAMPLING_START)

            lock.lock()
            try {
                sampledRecords = sampledRecords ++ response.sampledData
                numberOfClientsThatSampled += 1

                if (numberOfClientsThatSampled == Master.numOfRequiredConnections) {
                    MASTER_STATE = SAMPLING_PIVOT
                }
            } finally {
                lock.unlock()
            }

            Future.successful(Empty())
        }
    }
}