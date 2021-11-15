import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

object Master {
    def main(args: Array[String]): Unit = {
        val logger = LoggerFactory.getLogger(getClass)

        // argument handling
        if (args.length != 1) {
            logger.error("[*] master <slave #>")
            sys.exit(1)
        }

        val slaveNum = {
            try {
                args(0).toInt
            } catch {
                case e: Exception => {
                    logger.error("[*] master <slave #>")
                    sys.exit(1)
                }
            }
        }

        logger.info("[*] master with {0} slaves", slaveNum.toString)

        // connection phase (server required in master)
        new Connection(slaveNum, ExecutionContext.global)

        // sampling phase (server not required in master)

        // sort/partitioning phase (sever not required in master)

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
