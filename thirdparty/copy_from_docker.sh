#!/usr/bin/env bash

STARROCKS_BRANCH=2.4

docker create --name temp-sr-dev-env-$STARROCKS_BRANCH starrocks/dev-env:branch-$STARROCKS_BRANCH
mkdir -p /var/local/thirdparty
docker cp temp-sr-dev-env-$STARROCKS_BRANCH:/var/local/thirdparty/installed /var/local/thirdparty
docker rm temp-sr-dev-env-$STARROCKS_BRANCH