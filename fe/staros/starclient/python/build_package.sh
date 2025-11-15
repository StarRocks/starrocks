#!/bin/bash

PYTHON_OUTDIR=starclient/grpc_gen/
PROTO_SRC_PATH=proto
PYTHON_CMD=${PYTHON:-python}

mkdir -p $PYTHON_OUTDIR

# use protoc to generate protobuf python code
$PYTHON_CMD -m grpc_tools.protoc -I$PROTO_SRC_PATH --python_out=$PYTHON_OUTDIR --grpc_python_out=$PYTHON_OUTDIR $PROTO_SRC_PATH/*.proto || break
# fix protoc generated python code compatible issue with python3
$PYTHON_CMD my-2to3.py -wn --no-diffs starclient/grpc_gen/*.py

# parse version info based on the following two lines in pom.xml
#   <artifactId>starclient</artifactId>
#   <version>x.y.z</version>
version=`awk '/<artifactId>starclient<\/artifactId>/ { getline ; print; }' ../pom.xml  | sed 's|^.*<version>\(.*\)</version>.*$|\1|g'`

# fix incompatible `sed` command option on Mac and Linux
sedi=(-i)
case "$(uname)" in
  # For macOS, use two parameters
  Darwin*) sedi=(-i "")
esac
# update version info
sed "${sedi[@]}" -e "s|version='.*'|version='$version'|g" setup.py
$PYTHON_CMD setup.py bdist_wheel
