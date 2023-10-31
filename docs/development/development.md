# Build in Docker

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
  sh build.sh
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
  sh build.sh
  ```

## Third party tool

> We have integrated some tools in the image so that you can easily use them

- llvm
- clang
