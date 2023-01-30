#!/usr/bin/env bash

###################################################################
# This script is used to build StarRocks dev docker image on ubuntu
# Usage: 
#    sh build.sh -b branch {image}
###################################################################

set -eo pipefail

CURDIR=`dirname "$0"`
CURDIR=`cd $CURDIR; pwd`

# Check args
usage() {
    echo "
Usage: $0 [options] 

    Build StarRocks developement environment docker image on ubuntu.
    After this command execute successfully, will build image named starrocks/dev-ubuntu.

Optional options:
    -b, --branch                build for branch, default is main
    -h, --help                  print usage
    -i, --image                 image name, default is starrocks/dev-ubuntu

Examples:
    $0 starrocks
    $0 -b branch-2.2
"
    exit 0
}

OPTS=$(getopt \
  -n $0 \
  -o 'hb:' \
  -l 'help,branch:,' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"

HELP=0
BRANCH="main"
IMAGENAME="starrocks/dev-ubuntu"
while true; do
    case "$1" in
        -b | --branch) BRANCH="$2"; shift 2 ;;
        -h | --help) HELP=1; shift ;;
        -i | --image) IMAGENAME="$2"; shift 2 ;;
        --) shift ;  break ;;
        *) echo "Unexpected option: $1 - this should not happen." ; exit 1 ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit
fi

if [[ $# -eq 1 ]];then
    IMAGENAME=$1
fi

build_docker() {
    DOCKER_CONTEXT=${CURDIR}/../.build-dev-ubuntu
    # create directory to build docker image
    if [[ -d ${DOCKER_CONTEXT} ]]; then
        rm -rf ${DOCKER_CONTEXT}
    fi
    mkdir -p ${DOCKER_CONTEXT}/thirdparty

    # prepare the thirdparty to copy
    if [[ -z ${STARROCKS_THIRDPARTY} ]]; then
        STARROCKS_THIRDPARTY=${CURDIR}/../../thirdparty
    fi

    if [[ ! -f ${STARROCKS_THIRDPARTY}/installed/include/fast_float/fast_float.h ]]; then
        echo "Please set the valid thirdparty path in environment variable STARROCKS_THIRDPARTY"
        exit 1
    fi

    echo "Start coping thirdparty to the image build context"
    cp -rf ${STARROCKS_THIRDPARTY}/installed ${DOCKER_CONTEXT}/thirdparty/installed
    echo "Copy finished"

    # build docker image
    echo "Start building the image"
    docker build ${DOCKER_CONTEXT} -f ${CURDIR}/../dockerfiles/Dockerfile-dev-ubuntu -t $IMAGENAME:$BRANCH --no-cache
    echo "Building finished and clean the context"
    rm -rf ${DOCKER_CONTEXT}
}

build_docker

