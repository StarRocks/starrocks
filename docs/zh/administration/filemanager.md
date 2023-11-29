---
displayed_sidebar: "Chinese"
---

# 文件管理器

本文描述了如何使用文件管理器创建、查看和删除文件。StarRocks 允许用户将公钥、私钥和证书文件等文件上传并保存至系统内，进而在命令中引用或访问。

## 基本概念

**文件**：是指创建并保存在 StarRocks 中的文件。每个文件都有一个全局唯一的标识符 (`FileId`)。一个文件由数据库名称 (`database`)、类别 (`catalog`) 和文件名 (`file_name`) 共同定位。

只有拥有 System 级 FILE 权限的用户才可以创建和删除文件。当一个文件归属于一个数据库时，对该数据库拥有访问权限的用户都可以使用该文件。

## 前提条件

在使用文件管理器前，您需要做如下配置：

- FE 配置
  - `small_file_dir`：用于存放上传文件的目录。默认路径为 FE 运行目录的 small_files/ 目录。您需要在 **fe.conf** 文件中修改该配置项，然后重启 FE 后生效。
  - `max_small_file_size_bytes`：单个文件大小限制。单位为字节，默认为 1 MB。大于该参数值的文件无法创建。您可以通过 [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) 语句在线修改该配置项。
  - `max_small_file_number`：一个 StarRocks 集群支持的总文件数量。默认为 100。当创建的文件数量超过该参数值，后续的创建将会被拒绝。您可以通过 ADMIN SET CONFIG 语句在线修改该配置项。

> 说明：文件数量和大小的增加会导致 FE 内存使用量增加，非必要不推荐增大 `max_small_file_size_bytes` 和 `max_small_file_number` 的参数值。

- BE 配置：在 **be.conf** 文件中设置配置项 `small_file_dir`。`small_file_dir` 是用于存放文件管理器下载的文件的目录。默认值为 BE 运行目录的 `lib/small_files/`。

## 创建文件

使用 CREATE FILE 语句创建文件。具体的语法和参数等信息，参见 [CREATE FILE](../sql-reference/sql-statements/Administration/CREATE_FILE.md)。文件创建后会自动上传并持久化在 StarRocks 集群中。

## 查看文件

使用 SHOW FILE 语句查看保存在数据库中的文件的信息。具体的语法和参数等信息，参见 [SHOW FILE](../sql-reference/sql-statements/Administration/SHOW_FILE.md)。

## 删除文件

使用 DROP FILE 语句删除文件。具体的语法和参数等信息，参见 [DROP FILE](../sql-reference/sql-statements/Administration/DROP_FILE.md)。

## 使用文件

- 如 FE 要使用文件，SmallFileMgr 类会直接将 FE 内存中的数据保存为本地文件，存储在指定的目录中，并返回本地文件的路径供 FE 使用。
- 如 BE 要使用文件，BE 会通过 FE 的 HTTP 接口 **/api/get_small_file** 将文件下载到 BE 指定的目录中使用。同时，BE 也会在内存中记录当前已经下载过的文件的信息。当 BE 请求使用一个文件时，会先查看本地文件是否存在该文件并对该文件进行校验。如果校验通过，则返回本地文件路径。如果校验失败，则会删除本地文件，重新从 FE 下载。当 BE 重启时，会预先加载本地的文件到内存中。
