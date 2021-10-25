# 文件管理器

StarRocks 中的一些功能需要使用一些用户自定义的文件。比如用于访问外部数据源的公钥、密钥文件、证书文件等等。文件管理器提供这样一个功能，能够让用户预先上传这些文件并保存在 StarRocks 系统中，然后可以在其他命令中引用或访问。

## 基本概念

文件是指用户创建并保存在 StarRocks 中的文件。

一个文件由 `数据库名称（database）`、`分类（catalog）` 和 `文件名（file_name）` 共同定位。同时每个文件也有一个全局唯一的 id（file_id），作为系统内的标识。

文件的创建和删除只能由拥有 `admin` 权限的用户进行操作。某个文件归属于某一个的 database，则对该 database 拥有访问权限的用户都可以使用该文件。

## 具体操作

文件管理主要有三个命令：`CREATE FILE`，`SHOW FILE` 和 `DROP FILE`，分别为创建、查看和删除文件。这三个命令的具体语法可以通过连接到 StarRocks 后，执行 `HELP cmd;` 的方式查看帮助。

1. CREATE FILE

    该语句用于创建并上传一个文件到 StarRocks 集群。

    该功能通常用于管理一些其他命令中需要使用到的文件，如证书、公钥私钥等等。

    单个文件大小限制为 1MB。
    一个 StarRocks 集群最多上传 100 个文件。

    语法：

    ~~~sql
    CREATE FILE "file_name" [IN database]
    [properties]
    ~~~

    说明：

    * file_name:  自定义文件名。
    * database: 文件归属于某一个 db，如果没有指定，则使用当前 session 的 db。

    properties 支持以下参数:

    * url: 必须。指定一个文件的下载路径。当前仅支持无认证的 http 下载路径。命令执行成功后，文件将被保存在 StarRocks 中，该 url 将不再需要。
    * catalog: 必须。对文件的分类名，可以自定义。但在某些命令中，会查找指定 catalog 中的文件。比如例行导入中的，数据源为 kafka 时，会查找 catalog 名为 kafka 下的文件。
    * md5: 可选。文件的 md5。如果指定，会在下载文件后进行校验。

    Examples:

    * 创建文件 ca.pem ，分类为 kafka

    ~~~sql
    CREATE FILE "ca.pem"
    PROPERTIES
    (
        "url" = "https://test.bj.bcebos.com/kafka-key/ca.pem",
        "catalog" = "kafka"
    );
    ~~~

    * 创建文件 client.key，分类为 my_catalog

    ~~~sql
    CREATE FILE "client.key"
    IN my_database
    PROPERTIES
    (
        "url" = "https://test.bj.bcebos.com/kafka-key/client.key",
        "catalog" = "my_catalog",
        "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
    );
    ~~~

    文件创建成功后，文件相关的信息将持久化在 StarRocks 中。用户可以通过 `SHOW FILE` 命令查看已经创建成功的文件。

2. SHOW FILE

    该语句用于展示一个 database 内创建的文件

    语法：

    ~~~sql
    SHOW FILE [FROM database];
    ~~~

    说明：

    * FileId:     文件ID，全局唯一
    * DbName:     所属数据库名称
    * Catalog:    自定义分类
    * FileName:   文件名
    * FileSize:   文件大小，单位字节
    * MD5:        文件的 MD5

    Examples:

    查看数据库 my_database 中已上传的文件

    ~~~sql
    mysql> SHOW FILE FROM test;
    Empty set (0.00 sec)
    ~~~

3. DROP FILE

    该语句用于删除一个已上传的文件。

    语法：

    ~~~sql
    DROP FILE "file_name" [FROM database]
    [properties]
    ~~~

    说明：

    * file_name:  文件名。
    * database: 文件归属的某一个 db，如果没有指定，则使用当前 session 的 db。

    properties 支持以下参数:

    * catalog: 必须。文件所属分类。

    Examples:

    删除文件 ca.pem

    ~~~sql
    DROP FILE "ca.pem" properties("catalog" = "kafka");
    ~~~

## 实现原理

### 创建和删除文件

当用户执行 `CREATE FILE` 命令后，FE 会从给定的 URL 下载文件。并将文件的内容以 Base64 编码的形式直接保存在 FE 的内存中。同时会将文件内容以及文件相关的元信息持久化在 BDBJE 中。所有被创建的文件，其元信息和文件内容都会常驻于 FE 的内存中。如果 FE 宕机重启，也会从 BDBJE 中加载元信息和文件内容到内存中。当文件被删除时，会直接从 FE 内存中删除相关信息，同时也从 BDBJE 中删除持久化的信息。

### 文件的使用

如果是 FE 端需要使用创建的文件，则 SmallFileMgr 会直接将 FE 内存中的数据保存为本地文件，存储在指定的目录中，并返回本地的文件路径供使用。

如果是 BE 端需要使用创建的文件，BE 会通过 FE 的 http 接口 `/api/get_small_file` 将文件内容下载到 BE 上指定的目录中，供使用。同时，BE 也会在内存中记录当前已经下载过的文件的信息。当 BE 请求一个文件时，会先查看本地文件是否存在并校验。如果校验通过，则直接返回本地文件路径。如果校验失败，则会删除本地文件，重新从 FE 下载。当 BE 重启时，会预先加载本地的文件到内存中。

## 使用限制

因为文件元信息和内容都存储于 FE 的内存中。所以默认仅支持上传大小在 1MB 以内的文件。并且总文件数量限制为 100 个。可以通过参数配置项修改限制。

## 相关配置

1. FE 配置

    * `small_file_dir`：用于存放上传文件的路径，默认为 FE 运行目录的 `small_files/` 目录下。
    * `max_small_file_size_bytes`：单个文件大小限制，单位为字节。默认为 1MB。大于该配置的文件创建将会被拒绝。
    * `max_small_file_number`：一个 StarRocks 集群支持的总文件数量。默认为 100。当创建的文件数超过这个值后，后续的创建将会被拒绝。

    如果需要上传更多文件或提高单个文件的大小限制，可以通过 `ADMIN SET CONFIG` 命令修改 `max_small_file_size_bytes` 和 `max_small_file_number` 参数。但文件数量和大小的增加，会导致 FE 内存使用量的增加，非特殊原因不推荐大量上传文件。

2. BE 配置

    * `small_file_dir`：用于存放从 FE 下载的文件的路径，默认为 BE 运行目录的 `lib/small_files/` 目录下。
