import MasterState._
import config.ClientInfo
import scala.collection.mutable
import org.slf4j.LoggerFactory

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
        while (MASTER_STATE == SERVER_START) {Thread.sleep(500)}
        log.info("Successfully turned on master side server")

        log.info("Epsilon transition: SERVER_FINISH -> CONNECTION_START")
        MASTER_STATE = CONNECTION_START

        assert(numOfRequiredConnections > 0)
        log.info(s"started master server expecting ${numOfRequiredConnections} slave(s)")

        log.info("Connection phase start")
        log.info("Master Connection Barrier: CONNECTION_START <-> CONNECTION_FINISH")
        while (MASTER_STATE == CONNECTION_START) {Thread.sleep(500)}

        val connectionClass = new MasterConnectionDoneRequest(null)
        connectionClass.broadcastConnectionDone()
        log.info("Connection phase successfully finished")

        log.info("Epsilon transition: CONNECTION_FINISH -> SAMPLING_START")
        MASTER_STATE = SAMPLING_START

        log.info("Sampling phase start")
        val samplingClass = new Request_MasterSampleFirst(null)
        samplingClass.broadcastSampleStart()

        log.info("Master Sampling Barrier 1: SAMPLING_START <-> SAMPLING_PIVOT")
        while (MASTER_STATE == SAMPLING_START) {Thread.sleep(500)}

        MasterSortSampledRecords.pivotFromSampledRecords(Service_MasterSampleFirst.sampledRecords)
        val samplingSecondClass = new Request_MasterSampleSecond(null)
        samplingSecondClass.broadcastPivots()

        log.info("Master Sampling Barrier 2: SAMPLING_PIVOT <-> SAMPLING_FINISH")
        while (MASTER_STATE == SAMPLING_PIVOT) {Thread.sleep(500)}
        log.info("Sampling phase connection phase finished")

        log.info("Epsilon transition: SAMPLING_FINISH -> SORT_PARTITION_START")
        MASTER_STATE = SORT_PARTITION_START

        log.info("Sorting phase start")
        val sortingClass = new Request_MasterSort(null)
        sortingClass.broadcastSortStart()

        log.info("Master Sort/Partition Barrier: SORT_PARTITION_START <-> SORT_PARTITION_FINISH")
        while (MASTER_STATE == SORT_PARTITION_START) {Thread.sleep(500)}
        log.info("Sorting phase finished")

        log.info("Epsilon transition: SORT_PARTITION_FINISH -> MERGE_START")
        MASTER_STATE = MERGE_START

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
