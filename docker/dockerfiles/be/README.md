# Starrocks be/cn ubuntu image

## 1 Build Starrocks be/cn ubuntu image for k8s deployment
```
DOCKER_BUILDKIT=1 docker build -f be-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/be-ubuntu:<tag> ../../..
```
E.g.:
- Use artifact image to package runtime container
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-ubuntu:main -f be-ubuntu.Dockerfile -t be-ubuntu:main ../../..
```

- Use locally build artifacts to package runtime container
```shell
DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f be-ubuntu.Dockerfile -t ghcr.io/starrocks/starrocks/be-ubuntu:main ../../..
```

## 2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/be-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/starrocks/starrocks/be-ubuntu:main
```

## 3 CN image
Image built from previous steps can be used as compute node image, the only difference is the entrypoint script.
* `be_entrypoint.sh` : be start script
* `cn_entrypoint.sh` : cn start script
