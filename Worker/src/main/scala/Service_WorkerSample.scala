import Worker.WORKER_STATE
import WorkerState.{SAMPLING_FINISH, SAMPLING_START}
import protobuf.connect.{Empty, SamplingRequest, sampleWorkerServiceGrpc}

import scala.concurrent.Future

class Service_WorkerSample extends sampleWorkerServiceGrpc.sampleWorkerService {
    override def masterToWorkerSampleRequest(request: SamplingRequest): Future[Empty] = {
        assert(WORKER_STATE == SAMPLING_START)
        WORKER_STATE = SAMPLING_FINISH

        Future.successful {
            Empty()
        }
    }
}
