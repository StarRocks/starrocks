---
displayed_sidebar: docs
---

# Querybook

Querybook 支持对 StarRocks 的内部数据和外部数据进行查询和可视化处理。

## 前提条件

确保您已完成如下准备工作：

1. 克隆并下载 Querybook 库。

   ```SQL
   git clone git@github.com:pinterest/querybook.git
   cd querybook
   ```

2. 在项目根目录下的 `requirements` 文件夹里创建一个 `local.txt` 文件。

   ```SQL
   touch requirements/local.txt
   ```

3. 添加文件包。

   ```SQL
   echo -e "starrocks\nmysqlclient" > requirements/local.txt 
   ```

4. 启动容器。

   ```SQL
   make
   ```

## 集成

进入以下页面添加查询引擎。

```Plain
https://localhost:10001/admin/query_engine/
```

![Querybook](../../_assets/BI_querybook_1.png)

- 在 **Language** 里选择 **Starrocks**。
- 在 **Executor** 里选择 **sqlalchemy**。
- 在 **Connection_string** 里，按如下 StarRocks SQLAlchemy URI 格式来填写 URI：

  ```SQL
  starrocks://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>
  ```

  URI 参数说明如下：

  - `User`：用于登录 StarRocks 集群的用户名，如 `admin`。
  - `Password`：用于登录 StarRocks 集群的用户密码。
  - `Host`：StarRocks 集群的 FE 主机 IP 地址。
  - `Port`：StarRocks 集群的 FE 查询端口，如 `9030`。
  - `Catalog`：StarRocks 集群中的目标 Catalog。Internal Catalog 和 External Catalog 均支持。
  - `Database`：StarRocks 集群中的目标数据库。内部数据库和外部数据库均支持。
