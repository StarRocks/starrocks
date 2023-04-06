# StarRocks Dev Env Image
This directory contains dockerfiles to build the docker image for the development environment.

A StarRocks development environment contains all necessary development tools installed as well as prebuilt StarRocks third
party dependencies.

## 1 Build dev env image

### 1.1 Build ubuntu dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag> ../../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env.Dockerfile -t ghcr.io/starrocks/starrocks/dev-env-ubuntu:main ../../..
```
### 1.2 Build centos7 dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true --build-arg distro=centos7 -f dev-env.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-centos7:<tag> ../../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true --build-arg distro=centos7 -f dev-env.Dockerfile -t ghcr.io/starrocks/starrocks/dev-env-centos7:main ../../..
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag>
docker push ghcr.io/OWNER/starrocks/dev-env-centos7:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/dev-env-ubuntu:main
docker push ghcr.io/starrocks/starrocks/dev-env-centos7:main
```
