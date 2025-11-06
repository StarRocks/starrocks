  #!/bin/bash

  HOST_TYPE=${HOST_TYPE:-"IP"}
  FE_QUERY_PORT=${FE_QUERY_PORT:-9030}
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

  show_compute_nodes(){
      timeout 15 mysql --connect-timeout 2 -h $svc -P $FE_QUERY_PORT -u root --skip-column-names --batch -e 'SHOW COMPUTE NODES;'
  }

  parse_confval_from_cn_conf()
  {
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

  drop_my_self()
  {
      local svc=$1
      local start=`date +%s`
      local memlist=

      for ((i=0;i<3;++i))
      do
          log_stderr "try to drop myself($MY_SELF) from FE ..."
          memlist=`show_compute_nodes $svc`
          ret=$?
          if [[ $ret -eq 0 ]] ; then
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

  # graceful stop cn
  $STARROCKS_HOME/bin/stop_cn.sh -g
  # remove myself from FE
  svc_name=$1
  if [[ "x$svc_name" != "x" ]] ; then
      collect_env_info
      drop_my_self $svc_name
  fi
