object MasterSortSampledRecords {
    var pivotIndex: List[Int] = Nil
    var pivotList: List[String] = Nil

    def pivotFromSampledRecords(sampledRecords: List[String]) = {
        val sortedSampledRecords = sampledRecords.sortWith(_ < _)
        pivotIndex = for {
            i <- List.range(0, Master.numOfRequiredConnections)
        } yield {
            i * (sampledRecords.size / Master.numOfRequiredConnections)
        }
        pivotList = pivotIndex.map(sortedSampledRecords(_))
    }
}
