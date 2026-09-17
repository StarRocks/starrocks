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

# run one SQL statement against the FE service held in $svc (set by the caller)
run_sql(){
    timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e "$1"
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
        # Where the compute node lands is determined by two optional environment variables:
        #   KUBE_STARROCKS_MULTI_WAREHOUSE  the warehouse to join, created if missing; default_warehouse if unset.
        #   KUBE_STARROCKS_CNGROUP          the CN group inside that warehouse, created if missing.
        # When no CN group is given the CNGROUP clause is omitted and FE picks the group itself: it accepts an
        # omitted group only while the warehouse has exactly one CN group (whichever that is — the built-in
        # group can be dropped and replaced by a custom one), and rejects it once there are several. So a
        # warehouse with several CN groups requires KUBE_STARROCKS_CNGROUP to be set.
        local warehouse=${KUBE_STARROCKS_MULTI_WAREHOUSE:-default_warehouse}
        local cngroup=$KUBE_STARROCKS_CNGROUP
        local cngroup_clause=

        if [[ "x$KUBE_STARROCKS_MULTI_WAREHOUSE" != "x" ]] ; then
            run_sql "CREATE WAREHOUSE IF NOT EXISTS $warehouse;"
        fi
        if [[ "x$cngroup" != "x" ]] ; then
            run_sql "ALTER WAREHOUSE $warehouse ADD CNGROUP IF NOT EXISTS $cngroup;"
            cngroup_clause=" CNGROUP $cngroup"
        fi
        run_sql "ALTER SYSTEM ADD COMPUTE NODE \"$MY_SELF:$HEARTBEAT_PORT\" INTO WAREHOUSE $warehouse$cngroup_clause;"

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

    # If we infinitely retry to drop myself, it may cause the pod to be stuck in the Terminating state.
    for ((i=0;i<3;++i))
    do
        log_stderr "try to drop myself($MY_SELF) from FE ..."
        memlist=`show_compute_nodes $svc`
        ret=$?
        if [[ $ret -eq 0 ]] ; then
            # return code 0: no error
            selfinfo=`echo "$memlist" | grep -w "\<$MY_SELF\>" | awk '{printf("%s:%s\n", $2, $3);}'`
            if [[ "x$selfinfo" == "x" ]] ; then
                log_stderr "myself is not in fe cluster"
                return 0
            else
                log_stderr "drop my self $selfinfo ..."
                timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e "ALTER SYSTEM DROP COMPUTE NODE \"$selfinfo\";"
                break;
            fi
        else
            log_stderr "Got error $ret, sleep and retry ..."
            sleep $PROBE_INTERVAL
        fi
    done
}

exit_clean()
{
    log_stderr "Got SIGTERM, exit ..."
    exit 143
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
trap exit_clean SIGTERM

log_stderr "run start_cn.sh"

addition_args=
if [[ "x$LOG_CONSOLE" == "x1" ]] ; then
    # env var `LOG_CONSOLE=1` can be added to enable logging to console
    addition_args="--logconsole"
fi
$STARROCKS_HOME/bin/start_cn.sh $addition_args
ret=$?

if [[ $ret -eq 0 || $ret -eq 137 ]] ; then
    # The reason why we need to sleep here is to avoid the pod being killed by k8s before the preStop hook is exited.
    # If the CN subprocess fails to start, we also want the entrypoint script to exit as soon as possible.
    sleep 5
fi

# keep the same return code from start_cn.sh
exit $ret
