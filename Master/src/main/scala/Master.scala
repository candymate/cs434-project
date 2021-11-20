import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Master {
    def main(args: Array[String]): Unit = {
        val log = LoggerFactory.getLogger(getClass)

        // argument handling
        if (args.length != 1) {
            log.error("Wrong number of arguments")
            sys.exit(1)
        }

        val slaveNum = {
            try {
                args(0).toInt
            } catch {
                case e: Exception => {
                    log.error("Failed to parse args")
                    sys.exit(1)
                }
            }
        }

        log.info("Connection phase start")
        // connection phase (server required in master)
        val connection = new MasterConnection(slaveNum, ExecutionContext.global)
        log.info("Connection phase successfully finished")

        // sampling phase (server not required in master)
        log.info("Sampling phase start")
        new MasterSampling(connection.clientInfoMap, null, slaveNum)
        log.info("Sampling phase stop")

        // sort/partitioning phase (sever not required in master)

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
