import MasterState._
import channel.MasterToWorkerChannel
import config.ClientInfo
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext

object Master {
    @volatile var MASTER_STATE: MasterState = SERVER_START

    // key: machine order, value: ClientInfo
    var clientInfoMap: mutable.Map[Int, ClientInfo] = mutable.Map[Int, ClientInfo]()
    var numOfRequiredConnections: Int = 0

    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        log.info("Handling argument")
        MasterArgumentHandler.handleArgument(args)

        log.info("Start master server")
        val masterServer = new MasterServer(ExecutionContext.global)
        masterServer.start()

        log.info("Master server barrier: SERVER_START <-> SERVER_FINISH")
        while (MASTER_STATE == SERVER_START) {
            Thread.sleep(500)
        }
        log.info("Successfully turned on master side server")

        log.info("Epsilon transition: SERVER_FINISH -> CONNECTION_START")
        MASTER_STATE = CONNECTION_START
        Thread.sleep(5000)

        assert(numOfRequiredConnections > 0)
        log.info(s"started master server expecting ${numOfRequiredConnections} slave(s)")

        log.info("Connection phase start")
        log.info("Master Connection Barrier: CONNECTION_START <-> CONNECTION_FINISH")
        while (MASTER_STATE == CONNECTION_START) {
            Thread.sleep(500)
        }

        val connectionClass = new Request_MasterConnectionDone(Array())
        connectionClass.broadcastConnectionDone()
        log.info("Connection phase successfully finished")

        log.info("Epsilon transition: CONNECTION_FINISH -> SAMPLING_START")
        MASTER_STATE = SAMPLING_START
        Thread.sleep(5000)

        log.info("Sampling phase start")
        val samplingClass = new Request_MasterSampleFirst(Array())
        samplingClass.broadcastSampleStart()

        log.info("Master Sampling Barrier 1: SAMPLING_START <-> SAMPLING_PIVOT")
        while (MASTER_STATE == SAMPLING_START) {
            Thread.sleep(500)
        }

        MasterSortSampledRecords.pivotFromSampledRecords(Service_MasterSampleFirst.sampledRecords)
        val samplingSecondClass = new Request_MasterSampleSecond(Array())
        samplingSecondClass.broadcastPivots()

        log.info("Master Sampling Barrier 2: SAMPLING_PIVOT <-> SAMPLING_FINISH")
        while (MASTER_STATE == SAMPLING_PIVOT) {
            Thread.sleep(500)
        }
        log.info("Sampling phase connection phase finished")

        log.info("Epsilon transition: SAMPLING_FINISH -> SORT_PARTITION_START")
        MASTER_STATE = SORT_PARTITION_START
        Thread.sleep(5000)

        log.info("Sorting phase start")
        val sortingClass = new Request_MasterSort(Array())
        sortingClass.broadcastSortStart()

        log.info("Master Sort/Partition Barrier: SORT_PARTITION_START <-> SORT_PARTITION_FINISH")
        while (MASTER_STATE == SORT_PARTITION_START) {
            Thread.sleep(500)
        }
        log.info("Sorting phase finished")

        log.info("Epsilon transition: SORT_PARTITION_FINISH -> SHUFFLE_START")
        MASTER_STATE = SHUFFLE_START
        Thread.sleep(5000)

        log.info("Shuffling phase start")
        val shufflingClass = new Request_MasterShuffle(Array())
        shufflingClass.broadcastShuffleStart()

        log.info("Master Shuffle Barrier: SHUFFLE_START <-> SHUFFLE_FINISH")
        while (MASTER_STATE == SHUFFLE_START) {
            Thread.sleep(500)
        }
        log.info("Shuffling phase finished")

        log.info("Epsilon transition: SHUFFLE_FINISH -> MERGE_START")
        MASTER_STATE = MERGE_START
        Thread.sleep(5000)

        log.info("Merging phase start")
        val mergingClass = new Request_MasterMerge(Array())
        mergingClass.broadcastMergeStart()

        log.info("Master Merge Barrier: MERGE_START <-> MERGE_FINISH")
        while (MASTER_STATE == MERGE_START) {
            Thread.sleep(500)
        }
        log.info("Merging phase finished")

        Thread.sleep(5000)
        masterServer.stop()

        Thread.sleep(5000)
        MasterToWorkerChannel.closeMasterToWorkerChannelArray()

        log.info("SORTING FINISHED : SERVER CLOSE")

    }
}
