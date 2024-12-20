#!/usr/bin/env bash
# Copyright (C) dirlt

set -euo pipefail

version=$1
TAG="rel/release-${version}"
basedir="release-${version}"

# if we already have a local copy, use it.
if [ -d offline/${basedir} ]; then
    rm -rf ${basedir}
    cp -r -p offline/${basedir} ${basedir}
fi

function download {
    for f in "${FILES[@]}"
    do
        url=$URL/$f
        f=${basedir}/${f}
        if [ ! -f $f ]; then
            mkdir -p `dirname $f`
            echo "curl -L -S $url"
            curl -L -S $url > $f
        fi
    done
}

export FILES=(src/main/java/org/apache/hadoop/security/UserGroupInformation.java \
           src/main/java/org/apache/hadoop/ipc/Client.java \
           src/main/java/org/apache/hadoop/ipc/RPC.java)
export URL=https://raw.githubusercontent.com/apache/hadoop/${TAG}/hadoop-common-project/hadoop-common/
download

export FILES=(src/main/java/org/apache/hadoop/hdfs/protocol/datatransfer/sasl/SaslDataTransferClient.java)
export URL=https://raw.githubusercontent.com/apache/hadoop/${TAG}/hadoop-hdfs-project/hadoop-hdfs-client/
download
