---
displayed_sidebar: docs
description: "Generate presigned HTTP GET URLs from object URIs or FILE values using storage volume credentials."
sidebar_position: 90
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# Presigned URL functions

<Experimental />

Generate presigned HTTP GET URLs for objects in object storage, allowing clients such as model services to download objects during the validity period without providing an access key (AK) or secret key (SK).

Currently, only S3-compatible object storage, such as OSS, is supported.

| Function | Input | Return type |
| --- | --- | --- |
| [get_presigned_url](#get_presigned_url) | Object URI | VARCHAR |
| [get_presigned_file](#get_presigned_file) | FILE value | FILE |

## Syntax

```sql
VARCHAR get_presigned_url(VARCHAR storage_volume, VARCHAR uri [, BIGINT expiration_time])
FILE get_presigned_file(VARCHAR storage_volume, FILE file_expr [, BIGINT expiration_time])
```

## Common parameters

- `storage_volume`: the name of the storage volume whose credentials, endpoint, and region are used for signing. It must be a non-NULL string literal that names an enabled storage volume of the `S3` type. `builtin_storage_volume` is not allowed. See [CREATE STORAGE VOLUME](../../sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md).
- `expiration_time`: optional. The validity period of the URL in seconds, from `1` to `3600`. TINYINT, SMALLINT, INT, and BIGINT are accepted; other types require an explicit CAST. If this parameter is omitted or NULL, the default value of `3600` is used.

## Signing rules

- You must have the USAGE privilege on the storage volume.
- Signing is a local computation. The functions do not access the object, so they do not check whether the object exists or whether the credentials are allowed to read it.
- The URI to sign must be an absolute object URI in the format `<scheme>://<bucket>/<key>`. Supported schemes are defined by the BE configuration item `s3_compatible_fs_list`: by default `s3`, `s3a`, `s3n`, `oss`, `cos`, `cosn`, `obs`, `ks3`, and `tos`.
- An object URI to sign must not contain `?`; otherwise, presigning fails.
- If the storage volume uses temporary credentials, such as an instance profile or an assumed role, the URL stops working when those credentials expire, which can be earlier than `expiration_time`.
- Invalid argument values cause the query to fail with an error rather than return NULL.

## get_presigned_url

`uri` accepts a constant, column, or expression of the CHAR or VARCHAR type. Returns a VARCHAR value containing the presigned URL. If `uri` is NULL or an empty string, returns NULL. If `uri` is an `http://` or `https://` URL, presigning fails.

## get_presigned_file

`file_expr` accepts a [FILE](../../data-types/other-data-types/FILE.md) expression, such as the result of [to_file](to_file.md) or [try_to_file](try_to_file.md). Returns a FILE value. If `file_expr` is NULL, returns NULL.

Returns a new FILE with only its `uri` replaced by the presigned URL. All other fields remain unchanged. If `uri` is NULL, an empty string, or an HTTP/HTTPS URL, returns the FILE unchanged.

## Examples

The following examples use a storage volume named `media_volume`.

### Example 1: Presign an object with the default validity period

```sql
SELECT get_presigned_url('media_volume', 's3://my-bucket/images/cat.png') AS url;
```

The result is similar to the following. The credential, date, and signature differ in your environment.

```plaintext
https://my-bucket.s3.us-east-1.amazonaws.com/images/cat.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<access_key>%2F<date>%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=<timestamp>&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature>
```

### Example 2: Presign an object in OSS

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

### Example 3: Presign a FILE value

```sql
SELECT get_presigned_file('oss_volume',
    to_file(named_struct('uri', 'oss://zhangjiakou/1.pdf')));
```

Returns a new FILE whose `uri` is the presigned URL, with all other fields unchanged.

Example result:

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
