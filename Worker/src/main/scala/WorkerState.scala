object WorkerState extends Enumeration {
    type WorkerState = Value
    val CONNECTION_START, CONNECTION_FINISH,
    SAMPLING_START, SAMPLING_FINISH = Value
}
