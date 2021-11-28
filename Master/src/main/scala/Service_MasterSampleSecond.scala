import Master.MASTER_STATE
import MasterState.{SAMPLING_FINISH, SAMPLING_PIVOT}
import org.slf4j.{Logger, LoggerFactory}
import protobuf.connect.{Empty, samplingPivotToSamplingFinishMasterGrpc}

import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.Future

object Service_MasterSampleSecond {
    var numberOfClientsThatReplied: Int = 0

    class Service_MasterSampleSecond extends samplingPivotToSamplingFinishMasterGrpc.samplingPivotToSamplingFinishMaster {
        val log: Logger = LoggerFactory.getLogger(getClass)
        private val lock = new ReentrantLock()

        override def samplePartitionFinished(request: Empty): Future[Empty] = {
            assert(MASTER_STATE == SAMPLING_PIVOT)

            lock.lock()
            try {
                numberOfClientsThatReplied += 1
                if (numberOfClientsThatReplied == Master.numOfRequiredConnections) {
                    MASTER_STATE = SAMPLING_FINISH
                }
            } finally {
                lock.unlock()
            }

            Future.successful(
                Empty()
            )
        }
    }
}
