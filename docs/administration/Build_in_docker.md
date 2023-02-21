# Introduction of the dev-ubuntu image

## Image

| branch-name      | image-name                       |
| ---------------- | -------------------------------- |
| main             | starrocks/dev-ubuntu:main        |
| branch-2.5       | starrocks/dev-ubuntu:branch-2.5  |
| branch-2.4       | starrocks/dev-ubuntu:branch-2.4  |
| branch-2.3       | starrocks/dev-ubuntu:branch-2.3  |

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

When you met above error message, it may be caused by lacking of memory.
You should give more memory to the container.
8GB is enough.
