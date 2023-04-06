#!/bin/bash

# $prog <fe-svc-name>

# Editlog port: default 9010
EDIT_LOG_PORT=9010
# Query port: default 9030
QUERY_PORT=9030
# host_type, default "IP"
HOST_TYPE=${HOST_TYPE:-"IP"}
# FE leader
FE_LEADER=
# probe interval: 2 seconds
PROBE_INTERVAL=2
# timeout for probe leader: 120 seconds
PROBE_LEADER_POD0_TIMEOUT=30 # at most 15 attempts, no less than the times needed for an election
PROBE_LEADER_PODX_TIMEOUT=120 # at most 60 attempts

# myself as IP or FQDN
MYSELF=

STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
STARROCKS_HOME=${STARROCKS_ROOT}/fe
FE_CONFFILE=$STARROCKS_HOME/conf/fe.conf
EXIT_IN_PROGRESS=false

log_stderr()
{
    echo "[`date`] $@" >&2
}

parse_confval_from_fe_conf()
{
    # a naive script to grep given confkey from fe conf file
    # assume conf format: ^\s*<key>\s*=\s*<value>\s*$
    local confkey=$1
    local confvalue=`grep "\<$confkey\>" $FE_CONFFILE | grep -v '^\s*#' | sed 's|^\s*'$confkey'\s*=\s*\(.*\)\s*$|\1|g'`
    echo "$confvalue"
}

collect_env_info()
{
    # set POD_IP, POD_FQDN, POD_INDEX, EDIT_LOG_PORT, QUERY_PORT
    if [[ "x$POD_IP" == "x" ]] ; then
        POD_IP=`hostname -i | awk '{print $1}'`
    fi

    if [[ "x$POD_FQDN" == "x" ]] ; then
        POD_FQDN=`hostname -f`
    fi

    if [[ "x$HOST_TYPE" == "xFQDN" ]] ; then
        MYSELF=$POD_FQDN
    else
        MYSELF=$POD_IP # default type if not specified
    fi

    # example: fe-sr-deploy-1.fe-svc.kc-sr.svc.cluster.local
    POD_INDEX=`echo $POD_FQDN | awk -F'.' '{print $1}' | awk -F'-' '{print $NF}'`

    # edit_log_port from conf file
    local edit_port=`parse_confval_from_fe_conf "edit_log_port"`
    if [[ "x$edit_log_port" != "x" ]] ; then
        EDIT_LOG_PORT=$edit_log_port
    fi

    # query_port from conf file
    local query_port=`parse_confval_from_fe_conf "query_port"`
    if [[ "x$query_port" != "x" ]] ; then
        QUERY_PORT=$query_port
    fi
}

show_frontends()
{
    local svc=$1
    # ensure `mysql` command can be ended with 15 seconds
    # "show frontends" query will hang when there is no leader yet in the cluster
    timeout 15 mysql --connect-timeout 2 -h $svc -P $QUERY_PORT -u root --skip-column-names --batch -e 'show frontends;'
}

probe_leader_for_pod0()
{
    # possible to have no result at all, because myself is the first FE instance in the cluster
    local svc=$1
    local start=`date +%s`
    local has_member=false
    local memlist=
    while true
    do
        memlist=`show_frontends $svc`
        local leader=`echo "$memlist" | grep '\<LEADER\>' | awk '{print $2}'`
        if [[ "x$leader" != "x" ]] ; then
            # has leader, done
            log_stderr "Find leader: $leader!"
            FE_LEADER=$leader
            return 0
        fi

        if [[ "x$memlist" != "x" ]] ; then
            # has memberlist ever before
            has_member=true
        fi

        # no leader yet, check if needs timeout and quit
        log_stderr "No leader yet, has_member: $has_member ..."
        local timeout=$PROBE_LEADER_POD0_TIMEOUT
        if $has_member ; then
            # set timeout to the same as PODX since there are other members
            timeout=$PROBE_LEADER_PODX_TIMEOUT
        fi

        local now=`date +%s`
        let "expire=start+timeout"
        if [[ $expire -le $now ]] ; then
            if $has_member ; then
                log_stderr "Timed out, abort!"
                exit 1
            else
                log_stderr "Timed out, no members detected ever, assume myself is the first node .."
                # empty FE_LEADER
                FE_LEADER=""
                return 0
            fi
        fi
        sleep $PROBE_INTERVAL
    done
}

probe_leader_for_podX()
{
    # wait until find a leader or timeout
    local svc=$1
    local start=`date +%s`
    while true
    do
        local leader=`show_frontends $svc | grep '\<LEADER\>' | awk '{print $2}'`
        if [[ "x$leader" != "x" ]] ; then
            # has leader, done
            log_stderr "Find leader: $leader!"
            FE_LEADER=$leader
            return 0
        fi
        # no leader yet, check if needs timeout and quit
        log_stderr "No leader yet ..."

        local now=`date +%s`
        let "expire=start+PROBE_LEADER_PODX_TIMEOUT"
        if [[ $expire -le $now ]] ; then
            log_stderr "Timed out, abort!"
            exit 1
        fi

        sleep $PROBE_INTERVAL
    done
}

probe_leader()
{
    local svc=$1
    # find leader under current service and set to FE_LEADER
    if [[ "$POD_INDEX" -eq 0 ]] ; then
        probe_leader_for_pod0 $svc
    else
        probe_leader_for_podX $svc
    fi
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

start_fe_no_meta()
{
    # apply --host_type and --helper option
    local svc=$1
    local opts=""
    if [[ "x$HOST_TYPE" != "x" ]] ; then
        opts+=" --host_type $HOST_TYPE"
    fi

    if [[ "x$FE_LEADER" != "x" ]] ; then
        opts+=" --helper $FE_LEADER:$EDIT_LOG_PORT"

        local start=`date +%s`
        while true
        do
            log_stderr "Add myself($MYSELF:$EDIT_LOG_PORT) to leader as follower ..."
            mysql --connect-timeout 2 -h $FE_LEADER -P $QUERY_PORT -u root --skip-column-names --batch -e "ALTER SYSTEM ADD FOLLOWER \"$MYSELF:$EDIT_LOG_PORT\";"
            # check if added successful
            if show_frontends $svc | grep -q -w "$MYSELF" &>/dev/null ; then
                break;
            fi

            local now=`date +%s`
            let "expire=start+30" # 30s timeout
            if [[ $expire -le $now ]] ; then
                log_stderr "Timed out, abort!"
                exit 1
            fi

            log_stderr "Sleep a while and retry adding ..."
            sleep $PROBE_INTERVAL
        done
    fi

    if [[ "x$LOG_CONSOLE" == "x1" ]] ; then
        opts+=" --logconsole"
    fi
    log_stderr "first start with no meta run start_fe.sh with additional options: '$opts'"
    $STARROCKS_HOME/bin/start_fe.sh $opts
}

start_fe_with_meta()
{
    local opts=""
    if [[ "x$HOST_TYPE" != "x" ]] ; then
        opts+=" --host_type $HOST_TYPE"
    fi

    if [[ "x$LOG_CONSOLE" == "x1" ]] ; then
        opts+=" --logconsole"
    fi
    log_stderr "start with meta run start_fe.sh with additional options: '$opts'"
    $STARROCKS_HOME/bin/start_fe.sh $opts
}

svc_name=$1
if [[ "x$svc_name" == "x" ]] ; then
    echo "Need a required parameter!"
    echo "  Example: $0 <fe_service_name>"
    exit 1
fi

update_conf_from_configmap
if [[ -f "/opt/starrocks/fe/meta/image/ROLE" ]];then
    log_stderr "start fe with exist meta."
    start_fe_with_meta
else
    log_stderr "first start fe with meta not exist."
    collect_env_info
    probe_leader $svc_name
    start_fe_no_meta $svc_name
fi
