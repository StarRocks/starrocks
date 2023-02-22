# Starrocks fe ubuntu image

## 1 Build Starrocks fe ubuntu image for k8s deployment
```
DOCKER_BUILDKIT=1 docker build -f fe-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/fe-ubuntu:<tag> ../../..
```
E.g.:
- Use artifact image to package runtime container
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-ubuntu:main -f fe-ubuntu.Dockerfile -t fe-ubuntu:main ../../..
```

- Use locally build artifacts to package runtime container
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f fe-ubuntu.Dockerfile -t fe-ubuntu:main ../../..
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/fe-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/fe-ubuntu:main
```
