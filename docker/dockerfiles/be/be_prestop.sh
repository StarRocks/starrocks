#!/bin/bash

STARROCKS_ROOT=${STARROCKS_ROOT:-"/opt/starrocks"}
STARROCKS_HOME=${STARROCKS_ROOT}/be

# graceful stop be
$STARROCKS_HOME/bin/stop_be.sh -g
