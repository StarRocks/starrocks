# Starrocks development toolchains

This dockerfile produces a docker image which installs following toolchains:
- GCC-10.3.1
- JAVA-1.8.0
- MAVEN-3.6.3
- CMAKE-3.22.4
- CLANG-FORMAT

## 1 Build multiarch toolchains on centos7 and publish to dockerhub
```
DOCKER_BUILDKIT=1 docker buildx -f toolchains-centos7.Dockerfile --platform <platform_list> -t starrocks/toolchains-centos7:<tag> --push .
```
E.g.:
```shell
DOCKER_BUILDKIT=1 docker buildx -f toolchains-centos7.Dockerfile --platform linux/amd64,linux/arm64 -t starrocks/toolchains-centos7:20230306 --push .
```

## 2 Build a single platform toolchain for centos7
```
DOCKER_BUILDKIT=1 docker build -f toolchains-centos7.Dockerfile -t starrocks/toolchains-centos7:20230306 .
```
