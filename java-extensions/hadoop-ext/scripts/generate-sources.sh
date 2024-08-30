#!/usr/bin/env bash
# Copyright (C) dirlt

set -euo pipefail
SCRIPT_DIR=$(dirname $(realpath $0))
cd $SCRIPT_DIR

version=$1; shift
./download-release.sh ${version}

rm -rf src
cp -r release-${version}/src/ src
patch -p3 < patch0.txt

for f in `find src -type f`
do
    mkdir -p ../`dirname $f`
    cp $f ../$f
done
