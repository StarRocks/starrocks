#!/bin/bash

# Notes:
# There're several ENV variables used in the BE entrypoint script:
# * COREDUMP_ENABLED: when it's set to true and BE process is crashed, a coredump is generated and the BE process would be restarted;
# * DEBUG_MODE: when it's set to true, BE process is restarted always;

HOST_TYPE=${HOST_TYPE:-"IP"}
FE_QUERY_PORT=${FE_QUERY_PORT:-9030}
PROBE_TIMEOUT=60
PROBE_INTERVAL=2
HEARTBEAT_PORT=9050
MY_SELF=
MY_IP=`hostname -i`
MY_HOSTNAME=`hostname -f`
STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
export STARROCKS_HOME=${STARROCKS_ROOT}/be
BE_CONFIG=$STARROCKS_HOME/conf/be.conf


log_stderr()
{
    echo "[`date`] $@" >&2
}

update_conf_from_configmap()
{
    if [[ "x$CONFIGMAP_MOUNT_PATH" == "x" ]] ; then
        log_stderr 'Empty $CONFIGMAP_MOUNT_PATH env var, skip it!'
        return 0
    fi
    if ! test -d $CONFIGMAP_MOUNT_PATH ; then
        log_stderr "$CONFIGMAP_MOUNT_PATH not exist or not a directory, ignore ..."
        return 0
    fi
    local tgtconfdir=$STARROCKS_HOME/conf
    for conffile in `ls $CONFIGMAP_MOUNT_PATH`
    do
        log_stderr "Process conf file $conffile ..."
        local tgt=$tgtconfdir/$conffile
        if test -e $tgt ; then
            # make a backup
            mv -f $tgt ${tgt}.bak
        fi
        ln -sfT $CONFIGMAP_MOUNT_PATH/$conffile $tgt
    done
}

show_backends(){
    timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e 'SHOW BACKENDS;'
}

parse_confval_from_cn_conf()
{
    # a naive script to grep given confkey from cn conf file
    # assume conf format: ^\s*<key>\s*=\s*<value>\s*$
    local confkey=$1
    local confvalue=`grep "\<$confkey\>" $BE_CONFIG | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    echo "$confvalue"
}

collect_env_info()
{
    # heartbeat_port from conf file
    local heartbeat_port=`parse_confval_from_cn_conf "heartbeat_service_port"`
    if [[ "x$heartbeat_port" != "x" ]] ; then
        HEARTBEAT_PORT=$heartbeat_port
    fi

    if [[ "x$HOST_TYPE" == "xIP" ]] ; then
        MY_SELF=$MY_IP
    else
        MY_SELF=$MY_HOSTNAME
    fi

}

add_self()
{
    local svc=$1
    start=`date +%s`
    local timeout=$PROBE_TIMEOUT

    while true
    do
        log_stderr "Add myself ($MY_SELF:$HEARTBEAT_PORT) into FE ..."
        timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e "ALTER SYSTEM ADD BACKEND \"$MY_SELF:$HEARTBEAT_PORT\";"
        memlist=`show_backends $svc`
        if echo "$memlist" | grep -q -w "$MY_SELF" &>/dev/null ; then
            break;
        fi

        let "expire=start+timeout"
        now=`date +%s`
        if [[ $expire -le $now ]] ; then
            log_stderr "Time out, abort!"
            exit 1
        fi

        sleep $PROBE_INTERVAL

    done
}

svc_name=$1
if [[ "x$svc_name" == "x" ]] ; then
    echo "Need a required parameter!"
    echo "  Example: $0 <fe_service_name>"
    exit 1
fi

update_conf_from_configmap
collect_env_info
add_self $svc_name || exit $?
log_stderr "run start_be.sh"

if [[ "$COREDUMP_ENABLED" == "true" ]]; then
  # start inotifywait loop daemon to monitor core dump generation
  $STARROCKS_ROOT/upload_coredump.sh &
fi


addition_args=
if [[ "x$LOG_CONSOLE" == "x1" ]] ; then
    # env var `LOG_CONSOLE=1` can be added to enable logging to console
    addition_args="--logconsole"
fi

while true; do
  $STARROCKS_HOME/bin/start_be.sh $addition_args
  ret=$?

  # Enhanced logging for exit status analysis
  log_stderr "start_be.sh exited with return code: $ret"

  # Decode the exit status to provide more meaningful information
  if [[ $ret -eq 0 ]]; then
    log_stderr "Exit reason: Normal termination (exit code 0)"
  elif [[ $ret -ge 128 && $ret -le 255 ]]; then
    # Exit codes 128+n indicate termination by signal n
    signal_num=$((ret - 128))
    case $signal_num in
      1)  log_stderr "Exit reason: Terminated by SIGHUP (signal 1) - Hangup" ;;
      2)  log_stderr "Exit reason: Terminated by SIGINT (signal 2) - Interrupt" ;;
      3)  log_stderr "Exit reason: Terminated by SIGQUIT (signal 3) - Quit" ;;
      6)  log_stderr "Exit reason: Terminated by SIGABRT (signal 6) - Abort/Assert failure" ;;
      9)  log_stderr "Exit reason: Terminated by SIGKILL (signal 9) - Kill" ;;
      11) log_stderr "Exit reason: Terminated by SIGSEGV (signal 11) - Segmentation fault" ;;
      15) log_stderr "Exit reason: Terminated by SIGTERM (signal 15) - Termination" ;;
      *)  log_stderr "Exit reason: Terminated by signal $signal_num (exit code $ret)" ;;
    esac
  else
    log_stderr "Exit reason: Process exited with code $ret (non-signal termination)"
  fi

  if [[ $ret -ne 0 && "x$LOG_CONSOLE" != "x1" ]] ; then
      nol=50
      log_stderr "Last $nol lines of be.INFO ..."
      tail -n $nol $STARROCKS_HOME/log/be.INFO
      log_stderr "Last $nol lines of be.out ..."
      tail -n $nol $STARROCKS_HOME/log/be.out
  fi

  # Repeat launching BE process in the two scenarios:
  #  a. DEBUG_MODE is true;
  #  b. coredump is enabled, and the error code is SIGABRT(6) or SIGSEGV(11);
  # otherwise BE process should still exit.
  should_exit=true
  restart_reason=""

  # Check DEBUG_MODE condition
  if [[ "$DEBUG_MODE" == "true" ]]; then
    should_exit=false
    restart_reason="DEBUG_MODE is enabled"
    log_stderr "Restart decision: should_exit=false (reason: $restart_reason)"
  else
    log_stderr "Restart decision: DEBUG_MODE is not enabled (DEBUG_MODE=$DEBUG_MODE)"
  fi

  # Check COREDUMP_ENABLED condition for crash signals
  if [[ "$COREDUMP_ENABLED" == "true" && ($ret -eq 134 || $ret -eq 139) ]]; then
    should_exit=false
    if [[ $ret -eq 134 ]]; then
      restart_reason="COREDUMP_ENABLED and process crashed with SIGABRT (exit code 134)"
    elif [[ $ret -eq 139 ]]; then
      restart_reason="COREDUMP_ENABLED and process crashed with SIGSEGV (exit code 139)"
    fi
    log_stderr "Restart decision: should_exit=false (reason: $restart_reason)"
  else
    if [[ "$COREDUMP_ENABLED" == "true" ]]; then
      log_stderr "Restart decision: COREDUMP_ENABLED is true but exit code $ret is not a crash signal (134=SIGABRT, 139=SIGSEGV)"
    else
      log_stderr "Restart decision: COREDUMP_ENABLED is not enabled (COREDUMP_ENABLED=$COREDUMP_ENABLED)"
    fi
  fi

  # Final decision logging
  if [[ "$should_exit" == "true" ]]; then
    log_stderr "Final decision: should_exit=true - BE process will NOT be restarted"
    log_stderr "Exiting with return code: $ret"
    exit $ret
  else
    log_stderr "Final decision: should_exit=false - BE process WILL be restarted"
    log_stderr "Restart reason: $restart_reason"
  fi

  # Print a message indicating the failure
  log_stderr "starrocks_be process exited with status: $ret"
  echo "Restarting starrocks_be ..."

  # Wait for a few seconds before restarting
  sleep_interval=${BE_RESTART_WAIT_SECONDS:-5}
  echo "wait for $sleep_interval seconds ..."
  sleep $sleep_interval
done
