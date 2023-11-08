#!/bin/bash

#config
user=root
passwd=
fe_ip=127.0.0.1
http_port=8030
timeout=30
max_failed_time=5
stop_bin=/home/starrocks/fe/bin/stop_fe.sh
start_bin=/home/starrocks/fe/bin/start_fe.sh


failed_time=0
while true
do
    start_time=$(date +%s)
    curl http://${user}:${passwd}@${fe_ip}:${http_port}/api/show_proc?path=/statistic --max-time ${timeout} --silent -o /dev/null
    succ=$?
    end_time=$(date +%s)
    if [ !succ ] && [ $(($end_time - $start_time)) -ge ${timeout} ]; then
        failed_time=$(($failed_time + 1))
        echo "check failed, time used: $(($end_time - $start_time))"
    else
        failed_time=0
        echo "check successfully, time used: $(($end_time - $start_time))"
    fi

    if [ ${failed_time} -ge ${max_failed_time} ]; then
        echo "successive failed time is ${failed_time}, restart fe"
        sh ${stop_bin}
        sleep 10
        sh ${start_bin} --daemon
    fi

    sleep 10
done

