import Worker.WORKER_STATE
import WorkerState.{SAMPLING_FINISH, SORT_PARTITION_FINISH, SORT_PARTITION_START}
import protobuf.connect.{Empty, SamplingRequest, SortingRequest, sampleWorkerServiceGrpc, sortWorkerServiceGrpc}

import scala.concurrent.Future

class Service_WorkerSort extends sortWorkerServiceGrpc.sortWorkerService {
    override def masterToWorkerSortRequest(request: SortingRequest): Future[Empty] = {
        assert(WORKER_STATE == SORT_PARTITION_START)
        WORKER_STATE = SORT_PARTITION_FINISH

        WorkerSortAndPartition.pivotList = request.pivot.toList

        Future.successful {
            Empty()
        }
    }
}
