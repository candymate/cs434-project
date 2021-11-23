class MasterSortSampledRecords(sampledRecords: List[String], numberOfConnections: Int) {
    val sortedSampledRecords: List[String] = sampledRecords.sortWith(_ < _)
    val pivotIndex: List[Int] =
        for {
            i <- List.range(0, numberOfConnections)
        } yield {i * (sampledRecords.size / numberOfConnections)}
}
