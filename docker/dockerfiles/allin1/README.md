This docker file builds an ubuntu based all-in-one image that automatically starts then a complete Starrocks cluster stack(FE, BE and register BE to FE) at the time when container starts.

This is mainly used for developer to test starrocks locally.
Please override the [be.conf](be.conf) or [fe.conf](fe.conf) or fe.conf as you need.

### Build the all in one docker image:

**1) Use artifact image to package allin1 runtime container image**
```
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=<image> -f allin1-ubuntu.Dockerfile -t allin1-ubuntu:<tag> ../../../
```
**E.g.**
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-ubuntu:main -f allin1-ubuntu.Dockerfile -t allin1-ubuntu:main ../../../
```

**2) Use locally build artifacts to package allin1 runtime container image**
```
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=<local-repo-path> -f allin1-ubuntu.Dockerfile -t allin1-ubuntu:<tag> ../../../
```
**E.g.**
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f allin1-ubuntu.Dockerfile -t allin1-ubuntu:main ../../../
```

### [Allin1 HOWTO](allin1-HOWTO.md)
