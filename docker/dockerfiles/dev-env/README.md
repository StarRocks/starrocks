# Starrocks Ubuntu dev env image
This [dev-env-ubuntu.Dockerfile](dev-env-ubuntu.Dockerfile) build the docker image for the dev environment.
It builds and pre-install all the toolchains, dependence libraries, and maven dependencies that are needed for building Starrocks FE and BE.

## 1 Build Ubuntu dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag> ../../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/starrocks/starrocks/dev-env-ubuntu:main ../../..
```
## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/dev-env-ubuntu:main
```
