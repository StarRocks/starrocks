# Starrocks artifacts image

Artifact Package Stage packages the artifacts into a Busybox based image. The busybox base image is only 1MB, the packaged artifact image serves as a carrier to pass the Starrocks artifact to the next stage of docker build to package into various types of k8s deployment runtime images.


## 1 Build Starrocks aftifacts image for Ubuntu
Build the Starrocks artifacts fe & be and package them into a busybox basedimage

```
DOCKER_BUILDKIT=1 docker build -f artifact-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag> ../../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build -f artifact-ubuntu.Dockerfile -t ghcr.io/starrocks/starrocks/artifact-ubuntu:main ../../..
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/artifact-ubuntu:main
```
