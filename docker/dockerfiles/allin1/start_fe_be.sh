#!/bin/bash

log_stdin()
{
    echo "[`date`] $@" >&1
}

# Start FE.
cd $SR_HOME/fe/bin/
log_stdin "Start FE"
./start_fe.sh --host_type FQDN --daemon

# Start BE.
log_stdin "Start BE"
cd $SR_HOME/be/bin/
./start_be.sh --daemon


# Sleep until the cluster starts.
sleep 15;

# Fetch fqdn with the command suggested by AWS official doc: https://docs.aws.amazon.com/managedservices/latest/userguide/find-FQDN.html
MYFQDN=`hostname --fqdn`
log_stdin "Register BE ${MYFQDN} to FE"
mysql -uroot -h${MYFQDN} -P 9030 -e "alter system add backend '${MYFQDN}:9050';"


# health check the entire stack end-to-end and exit on failure.
while sleep 10; do
  PROCESS_STATUS=`mysql -uroot -h127.0.0.1 -P 9030 -e "show backends\G" |grep "Alive: true"`
  if [ -z "$PROCESS_STATUS" ]; then
        log_stdin "service has exited"
        exit 1;
  fi;
  log_stdin $PROCESS_STATUS
done
