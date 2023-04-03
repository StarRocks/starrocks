# Introduction of the dev-env-ubuntu image

## Image

The dev environment image was named as `dev-env`, and was a CentOS7 based image, since branch-2.5, Ubuntu-22.04 is recommended over CentOS7 distribution, the image is renamed to `dev-env-ubuntu` accordingly.

| branch-name      | image-name                          |
| ---------------- | ----------------------------------- |
| main             | starrocks/dev-env-ubuntu:latest     |
| branch-3.0       | starrocks/dev-env-ubuntu:3.0-latest |
| branch-2.5       | starrocks/dev-env-ubuntu:2.5-latest |
| branch-2.4       | starrocks/dev-env:branch-2.4        |
| branch-2.3       | starrocks/dev-env:branch-2.3        |

## Download the image

```shell
# download image from dockerhub
docker pull {image-name}
```

## How to use

- Run the container as usual

  ```shell
  # mount the docker and login
  docker run -it --name {branch-name} -d {image-name}
  docker exec -it {branch-name} /bin/bash

  # Download the code repository
  git clone https://github.com/StarRocks/starrocks.git

  # build the starrocks
  cd starrocks && ./build.sh
  ```

- Run the container by mounting the local path (**recommended**)

  - Avoid re-downloading java dependency
  - No need to copy the compiled binary package in starrocks/output from the container

  ```shell
  # download the code repository
  git clone https://github.com/StarRocks/starrocks.git

  # mount the docker and login
  docker run -it -v $(pwd)/.m2:/root/.m2 -v $(pwd)/starrocks:/root/starrocks --name {branch-name} -d {image-name}
  docker exec -it {branch-name} /bin/bash

  # build the starrocks
  cd /root/starrocks &&./build.sh
  ```

## Third party tool

> We have integrated some tools in the image so that you can easily use them

- llvm
- clang

## Required

Memory: 8GB+

## FAQ

1. Fail to compile StarRocks BE.

```shell
g++: fatal error: Killed signal terminated program cc1plus
compilation terminated.
```

When above error message is encounted, it may be caused by insufficient of memory.
Either give more memory to the container or reduce the parallism when running `./build.sh` by provideing a `-j <number_of_parallel_tasks>` option. Usually 8GB RAM is a good start.
