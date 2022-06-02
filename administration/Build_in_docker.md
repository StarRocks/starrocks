# dev-env image 使用说明

## image 下载

```shell
# 从 dockerhub 上下载 image
docker pull starrocks/dev-env:{version}
```

## starrocks 与 image 对应关系

| starrocks branch | image tag                     |
| ---------------- | ------------------------------|
| main             | starrocks/dev-env:main        |
| StarRocks-2.2.*  | starrocks/dev-env:branch-2.2  |
| StarRocks-2.1.*  | starrocks/dev-env:branch-2.1  |
| StarRocks-2.0.*  | starrocks/dev-env:branch-2.0  |
| StarRocks-1.19.* | starrocks/dev-env:branch-1.19 |

## 使用方式

- 不挂载本地盘

  ```shell
  docker run -it --name {container-name} -d starrocks/dev-env:{version}
  
  docker exec -it {container-name} /bin/bash
  
  # 在 container 内任意路径下执行 git clone starrocks
  git clone https://github.com/StarRocks/starrocks.git
  
  cd starrocks
  
  ./build.sh
  ```

- 挂载本地盘（建议使用）

  - 避免在 container 内重新下载 .m2 内 java dependency
  - 不用从 container 内 copy starrocks/output 内编译好的二进制包
  
  ```shell
  docker run -it \
  -v /{local-path}/.m2:/root/.m2 \
  -v /{local-path}/starrocks:/root/starrocks \
  --name {container-name} \
  -d starrocks/dev-env:{version}
  
  docker exec -it {container-name} /root/starrocks/build.sh
  ```

## 三方工具

image 内集安装的三方工具

- llvm
- clang
