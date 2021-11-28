import Worker.WORKER_STATE
import WorkerState._

import channel.WorkerToWorkerChannel
import java.net.InetAddress

object WorkerShuffle {
    var workerIdx = -1
    var workerNum = -1

    // all file list
    def shuffle(): Unit = {
        assert(WORKER_STATE == SHUFFLE_FINISH)

        val connInfo = WorkerToWorkerChannel.ipList zip WorkerToWorkerChannel.portList

        if (workerIdx == -1) {
            assert(!WorkerToWorkerChannel.ipList.isEmpty && !WorkerToWorkerChannel.portList.isEmpty)
            assert(WorkerToWorkerChannel.ipList.size == WorkerToWorkerChannel.portList.size)
            assert(workerNum == -1)

            workerIdx = connInfo.indexOf(
                (InetAddress.getLocalHost().getHostAddress(), WorkerArgumentHandler.optionalWorkerServerPort)
            )
            workerNum = WorkerToWorkerChannel.ipList.size
        }
        
        assert(workerIdx >= 0 && workerNum > 0 && workerIdx < workerNum)

        connInfo.foreach(conn => WorkerInterRequest.sendInterShuffle(conn._1, conn._2, workerIdx))
    }
}
