import Worker.WORKER_STATE
import WorkerState.{SAMPLING_SAMPLE, SAMPLING_FINISH}
import protobuf.connect.{Empty, PivotResult, samplingSampleToSamplingFinishWorkerGrpc}

import scala.concurrent.Future

object Service_WorkerSampleSecond {
    var pivotList: List[String] = Nil

    class Service_WorkerSampleSecond extends samplingSampleToSamplingFinishWorkerGrpc.samplingSampleToSamplingFinishWorker {
        override def pivotResult(request: PivotResult): Future[Empty] = {
            assert(WORKER_STATE == SAMPLING_SAMPLE)
            pivotList = request.pivotList.toList
            WORKER_STATE = SAMPLING_FINISH

            Future.successful {
                Empty()
            }
        }
    }
}
