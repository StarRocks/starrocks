# StarRocks Development Environment Toolchains

`Toolchains` docker image provides a ready to use development environment to build StarRocks project from scratch, e.g., building the thirdparty first, and the fe component and be component.

Toolchains dockerfile takes a linux distribution as the base image, installs necessary development tools, such as `gcc, cmake, java, maven, autoconf, automake, ccache, ...`. Developers can use this image to start to build StarRocks third party dependencies.

## 1 Build Toolchains for CentOS7
### 1.1 Build multiarch toolchains on CentOS7 and publish to docker hub
```
DOCKER_BUILDKIT=1 docker buildx build -f toolchains-centos7.Dockerfile --platform <platform_list> -t starrocks/toolchains-centos7:<tag> --push .
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker buildx build -f toolchains-centos7.Dockerfile --platform linux/amd64,linux/arm64 -t starrocks/toolchains-centos7:20230306 --push .
```

### 1.2 Build a single platform toolchain for CentOS7
```
DOCKER_BUILDKIT=1 docker build -f toolchains-centos7.Dockerfile -t starrocks/toolchains-centos7:20230306 .
```
## 2 Build Toolchains for Ubuntu
### 2.1 Build multiarch toolchains on Ubuntu and publish to docker hub
```
DOCKER_BUILDKIT=1 docker buildx build -f toolchains-ubuntu.Dockerfile --platform <platform_list> -t starrocks/toolchains-ubuntu:<tag> --push .
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker buildx build -f toolchains-ubuntu.Dockerfile --platform linux/amd64,linux/arm64 -t starrocks/toolchains-ubuntu:20230306 --push .
```

### 2.2 Build a single platform toolchain for Ubuntu
```
DOCKER_BUILDKIT=1 docker build -f toolchains-ubuntu.Dockerfile -t starrocks/toolchains-ubuntu:20230306 .
```
