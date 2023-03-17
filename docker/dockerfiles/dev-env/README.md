# Starrocks dev env image
This directory contains dockerfiles to build the docker image for the dev environment.

[dev-env-ubuntu.Dockerfile](dev-env-ubuntu.Dockerfile) build the docker image based on ubuntu22.04 for the dev environment.
[dev-env-centos7.Dockerfile](dev-env-centos7.Dockerfile) build the docker image based on centos7 for the dev environment.

It builds and pre-install all the toolchains, dependence libraries, and maven dependencies that are needed for building Starrocks FE and BE.

## 1 Build dev env image

### 1.1 Build ubuntu dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag> ../../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/starrocks/starrocks/dev-env-ubuntu:main ../../..
```
### 1.2 Build centos7 dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-centos7.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-centos7:<tag> ../../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-centos7.Dockerfile -t ghcr.io/starrocks/starrocks/dev-env-centos7:main ../../..
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
