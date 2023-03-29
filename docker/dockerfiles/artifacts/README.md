# StarRocks Artifacts Image

Artifact Package Stage packages the artifacts into a Busybox based image. The busybox base image is only 1MB, the packaged artifact image serves as a carrier to pass the Starrocks artifact to the next stage of docker build to package into various types of k8s deployment runtime images.


Before build the artifact image, a proper dev-env image should be choosen to build the FE/BE source code. Usually, if the artifact is created from `branch-x` for linux distribution `distro`, the corresponding `starrocks/dev-env-${distro}:${branch}-latest` should be used. E.g. to build the artifact from branch-3.0 for ubuntu distribution, the dev-env should be `starrocks/dev-env-ubuntu:branch-3.0-latest`.

## 1 Build StarRocks aftifacts image for Ubuntu
Build the Starrocks artifacts fe & be and package them into a busybox basedimage

```
DOCKER_BUILDKIT=1 docker build --build-arg builder=starrocks/dev-env-ubuntu:<branch>-latest -f artifact.Dockerfile -t ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag> ../../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build --build-arg builder=starrocks/dev-env-ubuntu:main-latest -f artifact.Dockerfile -t ghcr.io/starrocks/starrocks/artifact-ubuntu:main ../../..
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/artifact-ubuntu:main
```

## 3 Build StarRocks aftifacts image for CentOS7
Build the Starrocks artifacts fe & be and package them into a busybox basedimage

```
DOCKER_BUILDKIT=1 docker build --build-arg builder=starrocks/dev-env-centos7:<branch>-latest -f artifact.Dockerfile -t ghcr.io/OWNER/starrocks/artifact-centos7:<tag> ../../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build --build-arg builder=starrocks/dev-env-centos7:main-latest -f artifact.Dockerfile -t ghcr.io/starrocks/starrocks/artifact-centos7:main ../../..
```

## 4 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/artifact-centos7:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/artifact-centos7:main
```
