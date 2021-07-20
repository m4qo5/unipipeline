#!/bin/bash

echo ">>> $(basename ${BASH_SOURCE[0]})"

set -o errexit
set -o pipefail
set -o nounset



# INIT WORKING DIR
# ======================================================================================================
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ../
CWD="$(pwd)"

export PYTHONPATH=$CWD

echo ">> docker-compose up"
docker-compose -f $CWD/example/docker-compose.yml up -d

sleep 5

echo ">> example-kafka-csi"
pipenv run example-kafka-csi

echo ">> example-kafka-consumer-input"
pipenv run example-kafka-consumer-input &
pids[1]=$!

echo ">> example-kafka-consumer-after-cron"
pipenv run example-kafka-consumer-after-cron &
pids[1]=$!

echo ">> example-kafka-consumer-after-input"
pipenv run example-kafka-consumer-after-input &
pids[1]=$!

echo ">> example-kafka-consumer-cron"
pipenv run example-kafka-consumer-cron &
pids[1]=$!

sleep 5
echo ">> example-kafka-cron"
pipenv run example-kafka-cron &
pids[1]=$!


sleep 5
echo ">> example-kafka-producer"
pipenv run example-kafka-producer
pids[1]=$!

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done

echo ">> docker-compose down"
docker-compose -f $CWD/example/docker-compose.yml down -v

echo "DONE"
