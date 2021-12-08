import Worker.WORKER_STATE
import WorkerState.{SAMPLING_FINISH, SAMPLING_SAMPLE}
import protobuf.connect.{Empty, PivotResult, samplingSampleToSamplingFinishWorkerGrpc}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object Service_WorkerSampleSecond {
    var pivotList: List[String] = Nil
    val log = LoggerFactory.getLogger(getClass)

    class Service_WorkerSampleSecond extends samplingSampleToSamplingFinishWorkerGrpc.samplingSampleToSamplingFinishWorker {
        override def pivotResult(request: PivotResult): Future[Empty] = {
            assert(WORKER_STATE == SAMPLING_SAMPLE)
            log.info("Received sampled result")
            pivotList = request.pivotList.toList
            WorkerSortAndPartition.pivotList = pivotList
            WORKER_STATE = SAMPLING_FINISH

            Future.successful {
                Empty()
            }
        }
    }
}
