#!/bin/bash

STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
STARROCKS_HOME=${STARROCKS_ROOT}/fe
$STARROCKS_HOME/bin/stop_fe.sh