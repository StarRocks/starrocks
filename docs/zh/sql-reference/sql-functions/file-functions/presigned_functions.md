---
displayed_sidebar: docs
description: "使用存储卷凭证，为对象 URI 或 FILE 值生成预签名 HTTP GET URL。"
sidebar_position: 90
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# URL 预签名函数

<Experimental />

为对象存储中的对象生成预签名 HTTP GET URL，使模型服务等客户端无需提供 AK、SK，即可在 URL 有效期内下载对象。

目前暂时只支持兼容 S3 的对象存储，比如 `OSS` 等。

| 函数 | 输入 | 返回类型 |
| --- | --- | --- |
| [get_presigned_url](#get_presigned_url) | 对象 URI | VARCHAR |
| [get_presigned_file](#get_presigned_file) | FILE 值 | FILE |

## 语法

```sql
VARCHAR get_presigned_url(VARCHAR storage_volume, VARCHAR uri [, BIGINT expiration_time])
FILE get_presigned_file(VARCHAR storage_volume, FILE file_expr [, BIGINT expiration_time])
```

## 公共参数

- `storage_volume`：存储卷名称，签名使用该存储卷的凭证、Endpoint 和 Region。必须是非 NULL 的字符串字面量，对应一个已启用的 `S3` 类型存储卷。不允许使用 `builtin_storage_volume`。参见 [CREATE STORAGE VOLUME](../../sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md)。
- `expiration_time`：可选。URL 的有效期，单位为秒，取值范围为 `1` 到 `3600`。支持 TINYINT、SMALLINT、INT 和 BIGINT，其他类型需要显式 CAST。省略该参数或该参数为 NULL 时，使用默认值 `3600`。

## 签名规则

- 需要拥有该 Storage Volume 的 USAGE 权限。
- 签名是本地计算。函数不访问对象，因此不检查对象是否存在，也不检查凭证是否有权读取该对象。
- 需要签名的 URI 必须是对象的绝对 URI，格式为 `<scheme>://<bucket>/<key>`。支持的 scheme 由 BE 配置项 `s3_compatible_fs_list` 定义，默认为 `s3`、`s3a`、`s3n`、`oss`、`cos`、`cosn`、`obs`、`ks3` 和 `tos`。
- 需要签名的对象 URI 不能包含 `?`，否则预签名失败。
- 如果存储卷使用临时凭证（例如 Instance Profile 或 Assumed Role），URL 会在这些凭证过期时失效，可能早于 `expiration_time`。
- 参数值不合法时，查询报错，不返回 NULL。

## get_presigned_url

`uri` 可以是 CHAR 或 VARCHAR 类型的常量、列或表达式。返回包含预签名 URL 的 VARCHAR 值。`uri` 为 NULL 时直接返回 NULL。`uri` 为 `http://` 或 `https://` URL 时，预签名会失败。

## get_presigned_file

`file_expr` 接受 [FILE](../../data-types/other-data-types/FILE.md) 表达式，例如 [to_file](to_file.md) 或 [try_to_file](try_to_file.md) 的结果。返回 FILE 值。`file_expr` 为 NULL 时直接返回 NULL。

返回新的 FILE，仅将 uri 替换为预签名 URL，其他属性保持不变。`uri` 为 NULL 或 HTTP/HTTPS URL 时，原样返回 FILE。

## 示例

以下示例使用名为 `media_volume` 的存储卷。

### 示例 1：使用默认有效期对对象进行预签名

```sql
SELECT get_presigned_url('media_volume', 's3://my-bucket/images/cat.png') AS url;
```

返回结果类似如下。凭证、日期和签名因环境而异。

```plaintext
https://my-bucket.s3.us-east-1.amazonaws.com/images/cat.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<access_key>%2F<date>%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=<timestamp>&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature>
```

### 示例 2：对 OSS 上面一个对象进行预签名

```sql
CREATE STORAGE VOLUME oss_volume
TYPE = S3
LOCATIONS = ("s3://zhangjiakou/")
PROPERTIES (
    "aws.s3.endpoint" = "https://oss-cn-zhangjiakou.aliyuncs.com",
    "aws.s3.access_key" = "<ak>",
    "aws.s3.secret_key" = "<sk>"
);

select get_presigned_url('oss_volume', 'oss://zhangjiakou/1.pdf')\G;
```

```plaintext
*************************** 1. row ***************************
get_presigned_url('oss_volume', 'oss://zhangjiakou/1.pdf'): https://zhangjiakou.oss-cn-zhangjiakou.aliyuncs.com/1.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<ak>%2F20261113%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261113T070555Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature>
1 row in set (0.01 sec)
```

### 示例 3：对 FILE 进行预签名

```sql
SELECT get_presigned_file('oss_volume',
    to_file(named_struct('uri', 'oss://zhangjiakou/1.pdf')));
```

返回新的 FILE，其 `uri` 为预签名 URL，其他属性保持不变。

返回结果示例如下：

```json
{
  "uri": "https://zhangjiakou.oss-cn-zhangjiakou.aliyuncs.com/1.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<ak>%2F20261113%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261113T070555Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature>",
  "offset": null,
  "size": null,
  "content_type": null,
  "checksum": null,
  "inline": null
}
```
