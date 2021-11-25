object MasterState extends Enumeration {
    type MasterState = Value
    val CONNECTION_START, CONNECTION_FINISH,
    SAMPLING_START, SAMPLING_FINISH = Value
}
