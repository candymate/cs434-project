import WorkerArgumentHandler.outputFile
import WorkerState._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Worker {
    @volatile var WORKER_STATE: WorkerState = SERVER_START

    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        log.info("Handling argument")
        WorkerArgumentHandler.handleArgument(args)

        log.info("Start worker side server")
        val workerServer = new WorkerServer(ExecutionContext.global)
        workerServer.start()

        log.info("Worker server barrier: SERVER_START <-> SERVER_FINISH")
        while (WORKER_STATE == SERVER_START) {
            Thread.sleep(500)
        }
        log.info("Successfully turned on worker side server")

        log.info("Epsilon transition: SERVER_FINISH -> CONNECTION_START")
        WORKER_STATE = CONNECTION_START
        Thread.sleep(5000)

        log.info("Connection phase start")
        val workerConnection = new Request_WorkerConnection(null)
        workerConnection.initiateConnection()

        log.info("Worker Connection Phase Barrier: CONNECTION_START <-> CONNECTION_FINISH")
        while (WORKER_STATE == CONNECTION_START) {
            Thread.sleep(500)
        }
        log.info("Connection phase successfully finished")

        log.info("Epsilon transition: CONNECTION_FINISH -> SAMPLING_START")
        WORKER_STATE = SAMPLING_START
        Thread.sleep(5000)

        log.info("Sampling phase start")
        log.info("Worker Sampling Barrier 1: SAMPLING_START <-> SAMPLING_SAMPLE")
        while (WORKER_STATE == SAMPLING_START) {
            Thread.sleep(500)
        }

        Request_WorkerSamplingFirst.sendSampledDataToMaster()

        log.info("Worker Sampling Barrier 2: SAMPLING_SAMPLE <-> SAMPLING_FINISH")
        while (WORKER_STATE == SAMPLING_SAMPLE) {
            Thread.sleep(500)
        }
        Request_WorkerSamplingSecond.sendSampledDataToMaster()
        log.info("Sampling phase successfully finished")

        log.info("Epsilon transition: SAMPLING_FINISH -> SORT_PARTITION_START")
        WORKER_STATE = SORT_PARTITION_START
        Thread.sleep(5000)

        log.info("Sorting phase start")
        log.info("Worker Sort/Partition Barrier: SORT_PARTITION_START <-> SORT_PARTITION_FINISH")
        while (WORKER_STATE == SORT_PARTITION_START) {
            Thread.sleep(500)
        }
        WorkerSortAndPartition.sortAndPartitionFromInputFileList(WorkerArgumentHandler.inputFileArray,
            WorkerArgumentHandler.outputFile)
        Request_WorkerSort.sendSortFinished()
        log.info("Sorting phase finished")

        log.info("Epsilon transition: SORT_PARTITION_FINISH -> SHUFFLE_START")
        WORKER_STATE = SHUFFLE_START
        Thread.sleep(5000)

        log.info("Shuffling phase start")
        log.info("Worker Shuffle Barrier: SHUFFLE_START <-> SHUFFLE_FINISH")
        while (WORKER_STATE == SHUFFLE_START) {
            Thread.sleep(500)
        }
        WorkerShuffle.shuffle()
        while (WORKER_STATE == SHUFFLE_SERVICE) {
            Thread.sleep(500)
        }
        Request_WorkerShuffle.sendShuffleFinished()
        log.info("Shuffling phase finished")

        log.info("Epsilon transition: SHUFFLE_FINISH -> MERGE_START")
        WORKER_STATE = MERGE_START
        Thread.sleep(5000)

        log.info("Merging phase start")
        log.info("Worker Merge Barrier: MERGE_START <-> MERGE_FINISH")
        while (WORKER_STATE == MERGE_START) {
            Thread.sleep(500)
        }
        MergeUtil.mergeFiles(outputFile, MergeUtil.getListOfFiles(outputFile).map(x => new MultiFileRead(List(x))))
        Request_WorkerMerge.sendMergeFinished()

        Thread.sleep(5000)
        log.info("SORTING FINISHED SHUTTING DOWN WORKER")
        workerServer.stop()
        Thread.sleep(5000)

    }
}
