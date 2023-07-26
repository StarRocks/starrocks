#!/bin/bash

SQL_RETRY_INTERNAL=5
FE_HOME=$SR_HOME/fe
BE_HOME=$SR_HOME/be
MYCNF=$SR_HOME/director/my.cnf
BROKER_HOME=$SR_HOME/apache_hdfs_broker
source $FE_HOME/bin/common.sh
PREVIOUS_FQDN=
CURRENT_FQDN=`hostname -f`

PASSWD_ERROR_MSG="Password error, believed to have been changed, stop retrying!
If the root user password is changed, please re-run the container with '-e MYSQL_PWD=<root_password>'"
PERSISTENT_MSG=" * IMPORTANT NOTICE!

If FE/BE state needs to be persisted, please be sure the following directories are mounted:
* FE service meta: $FE_HOME/meta
* BE service storage: $BE_HOME/storage
"
HOSTNAME_MISMATCH_MSG="
Detect FE service hostname mismatch, FE service won't start.
This is probably caused by persisted fe/meta from outside container, but the container's
hostname is not fixed. If running with docker engine, use '-h <hostname>' to assign a
fixed hostname and restart.
"

MYHOST=127.0.0.1

log_stdout()
{
    echo "`date --rfc-3339=seconds` $@"
}

loginfo()
{
    log_stdout "INFO $@"
}

logwarn()
{
    log_stdout "WARN $@"
}

logerror()
{
    log_stdout "ERROR $@"
}

hang_and_die()
{
    sleeptime=15
    logerror "will force shutdown in $sleeptime seconds ..."
    sleep $sleeptime
    supervisorctl shutdown
    exit 1
}

fe_mysql_port()
{
    export_env_from_conf $FE_HOME/conf/fe.conf
    echo ${query_port:-"9030"}
}

be_heartbeat_service_port()
{
    export_env_from_conf $BE_HOME/conf/be.conf
    echo ${heartbeat_service_port:-"9050"}
}

broker_ipc_port()
{
    export_env_from_conf $BROKER_HOME/conf/apache_hdfs_broker.conf 
    echo ${broker_ipc_port:-"8000"}
}

check_fe_fqdn_mismatch()
{
    if [ -f $FE_HOME/meta/image/ROLE ] ; then
        previous_name=`grep '^name=' $FE_HOME/meta/image/ROLE | sed 's/name=//' | awk -F'_' '{print $1}'`
        if [ "$previous_name" == "127.0.0.1" ] ; then
            return
        fi
        current_name=`hostname -f`
        if [[ "$previous_name" != "$current_name" ]] ; then
            loginfo "hostname in FE meta: $previous_name"
            loginfo "current container hostname: $current_name"
            logerror "$HOSTNAME_MISMATCH_MSG"
            hang_and_die
        fi
    fi
}

# check fe service aliveness
check_fe_liveness()
{
    fequeryport=`fe_mysql_port`
    loginfo "checking if FE service query port:$fequeryport alive or not ..."
    while true
    do
        if nc -z -4 -w 5 $MYHOST $fequeryport ; then
            loginfo "FE service query port:$fequeryport is alive!"
            break
        else
            logwarn "FE service query port:$fequeryport is NOT alive yet!"
            sleep 2
        fi
    done
}

generate_my_cnf()
{
    loginfo "generate my.cnf file ..."
    fequeryport=`fe_mysql_port`
    cat > $MYCNF << EOF
[client]
user=root
host=$MYHOST
port=$fequeryport
column-names=FALSE
connect-timeout=2
EOF
}

exec_sql()
{
    mysql --defaults-file=$MYCNF --batch -e "$@"
}

exec_sql_with_column()
{
    mysql --defaults-file=$MYCNF --column-names --batch -e "$@"
}

exec_sql_with_retry()
{
    local sql="$@"
    while true
    do
        result=`exec_sql "$@" 2>&1`
        ret=$?
        if [ $ret -eq 0 ] ; then
            echo "$result"
            return 0
        else
            errcode=`echo $result | awk -F " " '{print $2}'`
            if [[ $errcode = '1045' || $errcode = '1064' ]] ; then
                logerror "$PASSWD_ERROR_MSG"
                return 1
            else
                logwarn "mysql command fails with error: '$result', will try again"
            fi
        fi
        sleep $SQL_RETRY_INTERNAL
    done
}

check_and_add_be()
{
    loginfo "check if need to add BE into FE service ..."
    while true
    do
        result=`exec_sql_with_retry "SHOW BACKENDS;"`
        ret=$?
        if [ $ret -ne 0 ] ; then
            echo "$result"
            hang_and_die
        else
            if echo "$result" | grep -q $MYHOST &>/dev/null ; then
                loginfo "BE service already added into FE service ... "
                return 0
            else
                beheartbeatport=`be_heartbeat_service_port`
                loginfo "Add BE($MYHOST:$beheartbeatport) into FE service ..."
                exec_sql_with_retry "ALTER SYSTEM ADD BACKEND '$MYHOST:$beheartbeatport';"
            fi
        fi
    done
}

check_and_add_broker()
{
    loginfo "check if need to add BROKER into FE service ..."
    while true
    do
        result=`exec_sql_with_retry "SHOW BROKER;"`
        ret=$?
        if [ $ret -ne 0 ] ; then
            hang_and_die
        else
            if echo "$result" | grep -q $MYHOST &>/dev/null ; then
                loginfo "broker service already added into FE service ... "
                return 0
            else
                brokerport=`broker_ipc_port`
                loginfo "Add BROKER($MYHOST:$brokerport) into FE service ..."
                exec_sql_with_retry "ALTER SYSTEM ADD BROKER allin1broker '$MYHOST:$brokerport';"
            fi
        fi
    done
}

loginfo "checking if need to perform auto registring Backend and Broker ..."
check_fe_fqdn_mismatch
check_fe_liveness
generate_my_cnf
check_and_add_be
check_and_add_broker
loginfo "cluster initialization DONE!"
loginfo "wait a few seconds for BE and Broker's heartbeat ..."
# allow heartbeat from BE/BROKER to FE
sleep 10
loginfo "StarRocks Cluster information details:"
exec_sql_with_column 'SHOW FRONTENDS\G'
exec_sql_with_column 'SHOW BACKENDS\G'
exec_sql_with_column 'SHOW BROKER\G'

loginfo
loginfo
loginfo "$PERSISTENT_MSG"
loginfo
loginfo "FE mysql query port: `fe_mysql_port`"
loginfo "FE http service port: 8080"
loginfo
loginfo "Enjoy the journal to StarRocks blazing-fast lake-house engine!"

while true
do
    st=`supervisorctl status`
    running=`echo "$st" | grep RUNNING | wc -l`
    bad=`echo "$st" | grep -v RUNNING | wc -l`
    if [ $bad -gt 0 ] ; then
        logwarn "has $bad services into non-RUNNING status!"
        logwarn "$st"
    fi
    sleep 5
done
