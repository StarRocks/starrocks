# 1. Starrocks Ubuntu dev env image
## 1.1 Build Ubuntu dev env image
```
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag> ../..
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker build --rm=true -f dev-env-ubuntu.Dockerfile -t ghcr.io/dengliu/starrocks/dev-env-ubuntu:latest ../..
```
## 1.2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/dev-env-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/dengliu/starrocks/dev-env-ubuntu:latest
```

# 2 Starrocks aftifacts image
## 2.1 Build Starrocks aftifacts image for Ubuntu
Build the Starrocks artifacts fe & be and package them into a busybox basedimage

```
DOCKER_BUILDKIT=1 docker build -f artifact-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag> ../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build -f artifact-ubuntu.Dockerfile -t ghcr.io/dengliu/starrocks/artifact-ubuntu:latest ../..
```

## 2.2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/artifact-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/dengliu/starrocks/artifact-ubuntu:latest
```


# 3. Starrocks be ubuntu image

## 3.1 Build Starrocks be ubuntu image for k8s deployment
```
DOCKER_BUILDKIT=1 docker build -f be-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/be-ubuntu:<tag> ../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build -f be-ubuntu.Dockerfile -t ghcr.io/dengliu/starrocks/be-ubuntu:latest ../..
```

## 3.2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/be-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/dengliu/starrocks/be-ubuntu:latest
```


# 4. Starrocks fe ubuntu image

## 4.1 Build Starrocks fe ubuntu image for k8s deployment
```
DOCKER_BUILDKIT=1 docker build -f fe-ubuntu.Dockerfile -t ghcr.io/OWNER/starrocks/fe-ubuntu:<tag> ../..
```
E.g.
```shell
DOCKER_BUILDKIT=1 docker build -f fe-ubuntu.Dockerfile -t ghcr.io/dengliu/starrocks/fe-ubuntu:latest ../..
```

## 4.2 Publish image to ghcr
```
docker push ghcr.io/OWNER/starrocks/fe-ubuntu:<tag>
```
E.g.:
```shell
docker push ghcr.io/dengliu/starrocks/fe-ubuntu:latest
```
