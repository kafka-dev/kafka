
REMOTE_KAFKA_HOME="~/kafka-perf"
REMOTE_KAFKA_LOG_DIR="~/tmp/kafka-logs"
#SIMULATOR_HOST
SIMULATOR_SCRIPT="$REMOTE_KAFKA_HOME/perf/run-simulator.sh"

# todo: some echos
# todo: talkative sleep

function kafka_startup() {
    ssh $REMOTE_KAFKA_HOST "cd $REMOTE_KAFKA_HOME; ./bin/kafka-server-start.sh config/server.properties 2>&1 > kafka.out" &
    sleep 10
}


function kafka_cleanup() {
    ssh $REMOTE_KAFKA_HOST "cd $REMOTE_KAFKA_HOME; ./bin/kafka-server-stop.sh" &
    sleep 10
    ssh $REMOTE_KAFKA_HOST "rm -rf $REMOTE_KAFKA_LOG_DIR" &
    sleep 10
}
