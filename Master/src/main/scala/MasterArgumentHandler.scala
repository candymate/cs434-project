import org.slf4j.LoggerFactory

object MasterArgumentHandler {
    val log = LoggerFactory.getLogger(getClass)

    def handleArgument(args: Array[String]) = {
        if (args.length != 1) {
            log.error("Wrong number of arguments")
            sys.exit(1)
        }

        require(args.length == 1)

        Master.numOfRequiredConnections = try {
            args(0).toInt
        } catch {
            case e: Exception => {
                log.error("Failed to parse args")
                sys.exit(1)
            }
        }
    }
}
