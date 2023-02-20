# Introduction of the dev-env image

## Download the image

```shell
# download image from dockerhub
docker pull starrocks/dev-env:{version}
```

## Version

| starrocks branch | image tag              |
| ---------------- | ---------------------- |
| main             | starrocks/dev-env:main |
| ...              | ...                    |

## How to use

- Run the container as usual

  ```shell
  docker run -it --name {container-name} -d starrocks/dev-env:{version}
  docker exec -it {container-name} /bin/bash
  # Run git clone starrocks in any path which in the container
  git clone https://github.com/StarRocks/starrocks.git
  cd starrocks
  ./build.sh
  ```

- Run the container by mounting the local path (**recommended**)

  - Avoid re-downloading java dependency
  - No need to copy the compiled binary package in starrocks/output from the container

  ```shell
  docker run -it \
  -v /{local-path}/.m2:/root/.m2 \
  -v /{local-path}/starrocks:/root/starrocks \
  --name {container-name} \
  -d starrocks/dev-env:{version}
  
  docker exec -it {container-name} /bin/bash
  cd /root/starrocks
  ./build.sh
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
