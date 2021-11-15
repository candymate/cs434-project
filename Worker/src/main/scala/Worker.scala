import org.slf4j.LoggerFactory

object Worker {
    def main(args: Array[String]): Unit = {
        val logger = LoggerFactory.getLogger(getClass)

        // argument handling


        // connection phase (server not required in worker)
        new Connection()

        // sampling phase (server required in worker)

        // shuffling (depending on implementation)

        // merging phase (server required in worker)

        // checking (server required in worker)
    }
}
