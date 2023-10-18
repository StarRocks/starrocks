#!/bin/bash

HOST_TYPE=${HOST_TYPE:-"IP"}
FE_QUERY_PORT=${FE_QUERY_PORT:-9030}
PROBE_TIMEOUT=60
PROBE_INTERVAL=2
HEARTBEAT_PORT=9050
MY_SELF=
MY_IP=`hostname -i`
MY_HOSTNAME=`hostname -f`
STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
STARROCKS_HOME=${STARROCKS_ROOT}/cn
CN_CONFIG=$STARROCKS_HOME/conf/cn.conf


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

show_compute_nodes(){
    timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e 'SHOW COMPUTE NODES;'
}

parse_confval_from_cn_conf()
{
    # a naive script to grep given confkey from cn conf file
    # assume conf format: ^\s*<key>\s*=\s*<value>\s*$
    local confkey=$1
    local confvalue=`grep "\<$confkey\>" $CN_CONFIG | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
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
        # if KUBE_STARROCKS_MULTI_WAREHOUSE environment variable is set, add compute node to the specified warehouse
        if  [[ "x$KUBE_STARROCKS_MULTI_WAREHOUSE" != "x" ]] ; then
            timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e \
              "CREATE WAREHOUSE IF NOT EXISTS $KUBE_STARROCKS_MULTI_WAREHOUSE;"
            timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e \
              "ALTER SYSTEM ADD COMPUTE NODE \"$MY_SELF:$HEARTBEAT_PORT\" INTO WAREHOUSE $KUBE_STARROCKS_MULTI_WAREHOUSE;"
        else
            timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e \
              "ALTER SYSTEM ADD COMPUTE NODE \"$MY_SELF:$HEARTBEAT_PORT\";"
        fi

        memlist=`show_compute_nodes $svc`
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

drop_my_self()
{
    local svc=$1
    local start=`date +%s`
    local memlist=

    while true
    do
        log_stderr "try to drop myself($MY_SELF) from FE ..."
        memlist=`show_compute_nodes $svc`
        ret=$?
        if [[ $ret -eq 0 ]] ; then
            # return code 0: no error
            selfinfo=`echo "$memlist" | grep -w "\<$MY_SELF\>" | awk '{printf("%s:%s\n", $2, $3);}'`
            if [[ "x$selfinfo" == "x" ]] ; then
                log_stderr "myself $selfinfo is not in fe cluster"
                return 0
            else
                log_stderr "drop my self $selfinfo ..."
                timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e "ALTER SYSTEM DROP COMPUTE NODE \"$selfinfo\";"
            fi
        else
            log_stderr "Got error $ret, sleep and retry ..."
            sleep $PROBE_INTERVAL
        fi
    done
}

exit_clean()
{
    log_stderr "Receives signal to exit ..."
    pidfile=$STARROCKS_HOME/bin/cn.pid
    if ! test -f $pidfile ; then
        log_stderr "Can't find $pidfile!"
    else
        pid=`cat $pidfile`
        if [[ "x$pid" == "x" ]] ; then
            log_stderr "Empty pid file!"
        else
            log_stderr "detect cn pid $pid exists ..."
            while true
            do
                if ps -p $pid &>/dev/null ; then
                    log_stderr "cn process $pid is still alive ..."
                    sleep $PROBE_INTERVAL
                else
                    log_stderr "cn process $pid dead."
                    break;
                fi
            done
        fi
    fi
    # remove myself from FE
    drop_my_self $svc_name
}

svc_name=$1
if [[ "x$svc_name" == "x" ]] ; then
    echo "Need a required parameter!"
    echo "  Example: $0 <fe_service_name>"
    exit 1
fi

collect_env_info
add_self $svc_name || exit $?
trap exit_clean SIGTERM

update_conf_from_configmap
log_stderr "run start_cn.sh"

addition_args=
if [[ "x$LOG_CONSOLE" == "x1" ]] ; then
    # env var `LOG_CONSOLE=1` can be added to enable logging to console
    addition_args="--logconsole"
fi
$STARROCKS_HOME/bin/start_cn.sh $addition_args
ret=$?
if [[ $ret -ne 0 && "x$LOG_CONSOLE" != "x1" ]] ; then
    nol=50
    log_stderr "Last $nol lines of cn.INFO ..."
    tail -n $nol $STARROCKS_HOME/log/cn.INFO
    log_stderr "Last $nol lines of cn.out ..."
    tail -n $nol $STARROCKS_HOME/log/cn.out
fi
exit $ret
