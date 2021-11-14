object Master {
    def main(args: Array[String]): Unit = {
        // argument handling

        // connection phase (server required in master)
        val connection = new Connection(3)

        // sampling phase (server not required in master)

        // sort/partitioning phase (sever not required in master)

        // merging phase (server not required in master)

        // checking (server required in master)

    }
}
