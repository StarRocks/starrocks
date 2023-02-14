## Build docker image
### build ubuntu artifact
```shell
DOCKER_BUILDKIT=1 docker build --build-arg builder=ghcr.io/dengliu/starrocks/starrocks-ubuntu-dev-env:latest -f ../docker/artifact/artifact.Dockerfile -t starrocks-artifacts:ubuntu ../..
```
### build ubuntu allin1
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACTIMAGE=starrocks-artifacts:ubuntu -f ../docker/allin1/allin1-ubuntu.Dockerfile -t starrocks-allin1:udffailure ../../celonis/docker/allin1
```

## Reproduce the error
Simply run the `debug.sh` script and it will try to create a docker container, register the udf function, run the udf function, and destroy the container for 100 times. You can check the logs to see if the image is flaky.
```shell
./debug.sh
```
