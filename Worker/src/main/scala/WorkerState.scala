object WorkerState extends Enumeration {
    type WorkerState = Value
    val SERVER_START, SERVER_FINISH,
    CONNECTION_START, CONNECTION_FINISH,
    SAMPLING_START, SAMPLING_SAMPLE, SAMPLING_FINISH,
    SORT_PARTITION_START, SORT_PARTITION_FINISH,
    SHUFFLE_START, SHUFFLE_SERVICE, SHUFFLE_FINISH,
    MERGE_START, MERGE_SERVICE, MERGE_FINISH = Value
}
