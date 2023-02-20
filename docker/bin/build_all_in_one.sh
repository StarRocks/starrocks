#!/usr/bin/env bash

##############################################################
# This script is used to compile StarRocks docker image
# Usage: 
#    sh build.sh -b branch {image}
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"/..; pwd`

# Check args
usage() {
  echo "
Usage: $0 <options>
  Optional options:
     -b                 build for branch

  Eg.
    $0 starrocks
    $0 -b branch-2.2
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o 'hb:' \
  -l 'help' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

HELP=0
BRANCH="main"
IMAGENAME="starrocks/starrocks"
while true; do
    case "$1" in
        -h) HELP=1; shift ;;
        --help) HELP=1; shift ;;
        -b) BRANCH=$2; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [[ $# -eq 1 ]];then
    IMAGENAME=$1
fi

cp dockerignore ${ROOT}/.dockerignore
docker build ${ROOT} -f ../dockerfiles/Dockerfile_all_in_one --build-arg BRANCH=$BRANCH -t $IMAGENAME
