---
displayed_sidebar: docs
sidebar_position: 5
description: "在共享数据集群上使用 Claude 和 StarRocks MCP 服务器分析复杂数据。"
---

# Claude + StarRocks MCP

在对象存储上搭建一个小型 StarRocks 集群，加载真实的公开数据集，并通过**Claude**用自然语言提问。**StarRocks MCP 服务器**Claude 自动发现 schema，编写 SQL（包括多表连接），并渲染图表。所有数据存储在对象存储中，运行于**MinIO**。

本教程涵盖以下内容：

- 以共享数据模式运行 StarRocks（1 FE + 1 CN）以及在 Docker 中运行 MinIO
- 创建 S3（MinIO）存储卷，实现存储与计算分离
- 加载 Olist 巴西电商数据集（8 张关联表）
- 将 StarRocks 和 AIStor MCP 服务器接入 Claude
- 向 Claude 提出自然语言问题并渲染图表

该数据集为**Olist 巴西电商**数据集 —— 包含 8 张关联表，适合真正复杂的连接查询。所有内容均可运行在**1 FE + 1 CN**，笔记本电脑或免费云服务层级上。

本文档包含大量信息，结构安排为：开头呈现分步操作内容，参考资料置于末尾。这样您可以先搭建环境并开始提问，之后再阅读相关背景详情。

***

## 前提条件

### Docker

- [Docker](https://www.docker.com/get-started/)（Docker Desktop 或 engine + compose）
- 为 Docker 分配约 4 GB 内存

### MySQL 客户端

需要一个 MySQL 客户端（例如 `mysql`）来运行 SQL 文件。StarRocks FE 服务器已提供该客户端，因此本指南中所有 SQL 命令均通过 `docker compose exec` 执行。

### uv

[`uv`](https://docs.astral.sh/uv/)用于运行 StarRocks MCP 服务器。

### Claude Code 或 Claude Desktop

[Claude Code](https://claude.com/product/claude-code)或 Claude Desktop 用于连接 MCP 服务器。

:::tip
本演示包含使用 Claude Code 的步骤。其他 LLM 也可与 MCP 服务器配合使用，但本演示仅在 Claude Code 上经过测试。如您在使用其他 LLM 时有任何体验，欢迎提交 issue 告知我们。
:::

### Olist 数据集

**Olist 数据集**在通过 `kagglehub` 加载数据时会自动下载（无需 Kaggle 账号）：[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)。

:::note
Apple Silicon：StarRocks 依赖 AVX2（x86）指令集；请使用 ARM 构建版本，否则可能出现较慢的模拟运行。
:::

***

## 术语

### MCP

模型上下文协议（MCP）是一种开放协议，允许 Claude 等 AI 助手发现并调用外部工具。本教程连接了两个 MCP 服务器：

- **StarRocks MCP 服务器**（`mcp-server-starrocks`）提供工具，让 Claude 能够读取 StarRocks 的模式并对其执行 SQL。
- **AIStor MCP 服务器**（`aistor`）提供工具，让 Claude 能够操作存储数据的 MinIO 对象存储——例如，浏览存储桶以及检查 StarRocks 写入的对象。

### FE

前端节点负责元数据管理、客户端连接管理、查询规划和查询调度。

### CN

计算节点负责在共享数据部署中执行查询计划。

***

## 安装前置条件

### `uv`

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows（PowerShell）
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# 或通过 Homebrew / pipx / pip 安装
brew install uv
# pipx install uv
# pip install uv
```

请参阅[官方 `uv` 安装指南](https://docs.astral.sh/uv/getting-started/installation/)了解其他选项。安装完成后，验证其是否在您的 `PATH` 中：

```bash
uv --version
```

### Docker

[https://www.docker.com/get-started/](https://www.docker.com/get-started/)

### Claude Code

[https://claude.com/product/claude-code](https://claude.com/product/claude-code)

***

## 克隆演示仓库

克隆[https://github.com/StarRocks/demo](https://github.com/StarRocks/demo)：

```bash
gh repo clone StarRocks/demo
```

或

```bash
git clone git@github.com:StarRocks/demo.git
```

:::note
该目录仅包含让您快速上手所需的内容。它有意*不包含*预设查询、没有预期答案、也没有操作说明——这样 Claude 展示的所有内容都是基于实时模式推理得出的，而非从检出的材料中召回的。
:::

***

## 启动 StarRocks 和 MinIO

```bash
cd demo/documentation-samples/MCP
docker compose up --detach --wait --wait-timeout 120
```

一个前端节点（FE）、一个计算节点（CN）和 MinIO 在本地启动。

检查 MinIO、FE 和 CN 服务的健康状态：

```bash
docker compose ps -a --format "table {{.Service}}\t{{.Status}}"
```

:::tip
如果 CN 尚未报告健康状态，请等待几秒钟后再次检查——它是最后启动的服务。
:::

***

## 在 MinIO 中创建存储桶

在以下地址打开 MinIO 控制台[http://localhost:9001](http://localhost:9001)（登录账号 `miniouser` / `M!n10R0cks`），点击**创建存储桶**，并创建存储桶 `my-starrocks-bucket`。

***

## 创建存储卷

文件 `storage_volume.sql` 会在您在上一步中创建的存储桶中创建一个 StarRocks 存储卷，并将其设置为默认值。文件内容将在本教程末尾详细说明；现在，请运行它：

```bash
docker compose exec -T starrocks-fe \
  mysql -P9030 -h127.0.0.1 -uroot < storage_volume.sql
```

此操作必须成功（且该卷必须为默认卷），才能进行任何 `CREATE TABLE`。

```sql
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
IsDefault: true
 Location: s3://my-starrocks-bucket/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
```

***

## 创建表

```bash
docker compose exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot < olist_schema.sql
```

:::tip
如果您看到错误 `The specified bucket does not exist`，您可能跳过了上一步的某个部分。请打开 MinIO UI 并创建上面指定的存储桶。
:::

:::tip
这是试用 MCP 服务器的好时机。从当前目录启动 Claude Code，允许两个 MCP 服务器（`aistor` 和 `mcp-server-starrocks`），然后让 Claude 列出数据库并描述 `olist` 数据库中的模式。
:::

***

## 加载数据

使用以下方式从 Kaggle 下载 Olist CSV 文件：**kagglehub** — 匿名访问，无需 Kaggle 账户或 API 令牌。它会在本地缓存文件并打印文件所在的文件夹。

下载数据并将其所在文件夹捕获为 `CSV_DIR`。kagglehub 会将版本警告打印到 stdout，因此使用 `tail -n1` 获取最后一行（路径）：

```bash
export CSV_DIR="$(uv run --with "kagglehub==0.3.12" python \
  -c "import kagglehub; print(kagglehub.dataset_download('olistbr/brazilian-ecommerce'))" \
  | tail -n1)"
echo "$CSV_DIR"   # sanity check: should be a .../brazilian-ecommerce/versions/N path
```

:::note
`kagglehub==0.3.12` 的固定版本是有意为之：较新版本（1.0.x）会拉取一个 `kagglesdk` 构建，导致导入失败（`ModuleNotFoundError: kagglesdk.competitions.legacy`）。0.3.12 可匿名下载公共数据集且运行正常。kagglehub 会缓存文件，因此重复运行代价很低。
:::

然后运行加载器（`CSV_DIR` 已导出）：

```bash
FE_HTTP_PORT=8040 bash load_olist.sh
```

`FE_HTTP_PORT=8040` 直接向 CN 发送 Stream Load，避免 `starrocks-cn` 主机名重定向（无需编辑 `/etc/hosts` / 无需 sudo）。脚本在末尾打印行数检查结果。

***

## 将 MCP 服务器连接到 Claude

```bash
cp .env.example .env
```

`.mcp.json` 定义了两个 MCP 服务器 — `mcp-server-starrocks`（通过 `uv` 从 GitHub 运行，因此**无需手动安装任何内容**）和 `aistor`（通过 Docker 运行）— 两者均读取 `.env`。从此目录启动 Claude Code（或将相同的配置块添加到 Claude Desktop 的配置中）。

:::note
**在首次启动时批准服务器。** 来自 `.mcp.json` 的项目范围服务器*不* 会自动受信任 — Claude Code 会在*"发现新的 MCP 服务器 — 是否批准？"* 您首次在此处启动时提示。请接受。（预批准信息存储在 `.claude/settings.local.json` 中，该文件已被 gitignore，因此**在全新克隆中不存在** — 这是预期行为；通过提示批准会重新创建该文件。）

**使用 `/mcp` 确认连接。** 在 Claude Code 中运行 `/mcp`，检查 `mcp-server-starrocks` 和 `aistor` 是否均显示**已连接**。这是权威的状态检查 — 如果某个服务器未连接，无论您如何请求，其工具都不会加载。

**首次启动较慢。** 首次 `uv run` 会下载服务器的依赖项（pyarrow、kaleido 等，约 115 MB），因此 StarRocks 服务器首次启动可能需要几分钟。后续启动速度很快。
:::

一旦 `/mcp` 显示两个服务器均已连接，请确认工具可用（例如询问*"StarRocks MCP 服务器提供哪些工具？"*）。

StarRocks 集群可通过 MySQL 协议以 `root`（无密码）在 `localhost:9030` 访问，数据库为 `olist`。

***

## 提问

用自然语言向 Claude 提问关于数据的问题 — 先让它熟悉数据，例如*"这个数据库中有哪些表，它们之间有什么关联？"*，然后从那里继续探索。Claude 会检查实时模式并自行编写 SQL。

### 建议问题

这些是演示视频中按顺序提出的问题。每个问题都建立在上一个问题的基础上——从第一个开始，让 Claude 确定方向，然后依次往下：

- *这里有哪些数据库和表？*
- *这些表是如何关联在一起的？*
- *这些数据实际存储在哪里？*
- *按客户州绘制订单图表。*
- *运送距离越远，费用越高吗？*
- *添加第二行——每公斤运费——以将重量与距离分开。*
- *您是如何构建该查询的？*
- *延迟交货会影响我们的评价吗？*
- *您使用的是预设答案，还是每个数字都来自实时表格？*

:::tip
**图表：** 在 中请求交互式图表 **HTML** 格式，StarRocks MCP 服务器将其写入 `DEMO_Output/`（由 `STARROCKS_CHART_OUTPUT_DIR` 在 `.env` 中设置），并在行内显示静态预览——在浏览器中打开 HTML 文件即可悬停 / 缩放 / 平移。
:::

***

## 故障排除

- **StarRocks MCP 工具无法加载 / 服务器"仍在连接中"：** 项目服务器从未获得批准。运行 `/mcp` — 如果它们不是 **已连接**，从此目录重新启动 Claude Code 并接受 *"发现新的 MCP 服务器 — 是否批准？"* 提示（或将 `"enabledMcpjsonServers": ["mcp-server-starrocks", "aistor"]` 添加到 `.claude/settings.local.json`）。您 **不**无需单独安装服务器 — `.mcp.json` 通过 `uv` 从 GitHub 运行它。
- **首次启动需要几分钟：** 第一次 `uv run` 会下载服务器的依赖项（约 115 MB）。这是预期中的一次性操作，不是失败。
- **`mysql` 客户端因 `Authentication plugin 'mysql_native_password' cannot be loaded` 失败：** 这是 Homebrew `mysql` 客户端的一个怪癖，而非 StarRocks 的问题。请通过容器执行查询：`docker compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW DATABASES;"`。
- **`CREATE TABLE` 挂起/超时：** CN 的 AWS SDK 正在探测 EC2 元数据服务。compose 文件已在 `starrocks-cn` 上设置了 `AWS_EC2_METADATA_DISABLED=true`；如果您使用自己的 compose 文件，请添加它。
- **Stream Load 重定向错误：** 使用 `FE_HTTP_PORT=8040`（如上所述）直接发布到 CN，或将 `127.0.0.1 starrocks-cn` 添加到 `/etc/hosts`。
- **`order_reviews` 拒绝的行：** 自由文本注释列包含嵌入的换行符；请使用 `load_olist.sh` 中注明的 LEAN 变体。

***

## 摘要

在本教程中，您将：

- 在 Docker 中部署了 StarRocks 共享数据模式（1 个 FE + 1 个 CN）和 MinIO
- 创建了 S3 (MinIO) 存储卷并将其设置为默认
- 加载了 Olist 巴西电商数据集（8 个相关表）
- 将 StarRocks 和 AIStor MCP 服务器连接到 Claude
- 用简单的英语向 Claude 提问，让它自动发现模式、编写 SQL 并渲染图表

构建/设置材料——旁白演示和视频脚本——单独维护，以使此环境不含预设答案：

- **StarRocks MCP 服务器：** [https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)

***

## 此处包含的内容

演示文件位于 [StarRocks/demo](https://github.com/StarRocks/demo) 仓库的 `documentation-samples/MCP` 目录中：

| 文件 | 说明 |
|---|---|
| `CLAUDE.md` | 向 Claude Code 发出的指令，要求其展示工作过程、依赖模式而非外部信息等。建议阅读此文件。 |
| `.claude/settings.json` | 内容为 `"autoMemoryEnabled": false`，以防止 Claude 记住之前会话中提出的问题（及答案）。如有需要可删除；此文件的目的是防止作者的问题影响您的实验。 |
| `docker-compose.yml` | StarRocks 共享数据快速入门（1 FE + 1 CN + MinIO），已针对笔记本电脑/Docker 使用进行修补。 |
| `storage_volume.sql` | 创建 S3（MinIO）存储卷并将其设为默认值。 |
| `olist_schema.sql` | 8 张（+1 张可选）Olist 表的 DDL。 |
| `load_olist.sh` | Olist CSV 的 Stream Load + 行数检查。 |
| `.mcp.json` | 将 StarRocks 和 AIStor MCP 服务器连接到 Claude。 |
| `.env.example` | 凭据/端点模板——将 `cp` 复制为 `.env` 并按需编辑。 |

***

## `storage_volume.sql` 说明

`storage_volume.sql` 执行两项操作：提高超时时间，然后创建并激活存储卷。

首先增加 tablet 创建的超时时间并创建存储卷：

```sql
-- 默认值为 10 秒，对于对象存储 tablet 创建而言过于紧张
ADMIN SET FRONTEND CONFIG ('tablet_create_timeout_second'='60');

CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES (
        "enabled" = "true",
        "aws.s3.endpoint" = "minio:9000",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.use_instance_profile" = "false",
        "aws.s3.use_aws_sdk_default_behavior" = "false"
    );
```

然后将新存储卷设为默认值并显示其详细信息：

```sql
SET s3_volume AS DEFAULT STORAGE VOLUME;   -- REQUIRED before CREATE DATABASE/TABLE
DESC STORAGE VOLUME s3_volume\G            -- verify: enabled=true, IsDefault=true
```

***

## 更多信息

- StarRocks 共享数据快速入门——[shared-data](shared-data.md)
- StarRocks MCP 服务器——[https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)
- AIStor MCP 服务器——[https://github.com/minio/mcp-server-aistor](https://github.com/minio/mcp-server-aistor)
- Olist 数据集——[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

***

## 致谢与许可

- **Olist Brazilian E-Commerce** 数据集来自 Kaggle，授权协议为 **CC BY-NC-SA 4.0（非商业用途）**——此处用于教育演示目的；版权归属 Olist。商业再利用前请确认相关条款。
- StarRocks 及 StarRocks MCP 服务器为开源项目（Apache-2.0 / 项目许可证）。
- MinIO 和 `mcp-server-aistor` © MinIO, Inc.
