---
displayed_sidebar: docs
description: "ストレージボリュームの認証情報を使い、オブジェクト URI または FILE 値から署名付き HTTP GET URL を生成します。"
sidebar_position: 90
---

import Experimental from '../../../_assets/commonMarkdown/_experimental.mdx'

# 署名付き URL 関数

<Experimental />

オブジェクトストレージ上のオブジェクトに対する署名付き HTTP GET URL を生成します。モデルサービスなどのクライアントは、アクセスキー (AK) やシークレットキー (SK) を提供せずに、URL の有効期間内にオブジェクトをダウンロードできます。

現在、OSS などの S3 互換オブジェクトストレージのみをサポートしています。

| 関数 | 入力 | 戻り値の型 |
| --- | --- | --- |
| [get_presigned_url](#get_presigned_url) | オブジェクト URI | VARCHAR |
| [get_presigned_file](#get_presigned_file) | FILE 値 | FILE |

## 構文

```sql
VARCHAR get_presigned_url(VARCHAR storage_volume, VARCHAR uri [, BIGINT expiration_time])
FILE get_presigned_file(VARCHAR storage_volume, FILE file_expr [, BIGINT expiration_time])
```

## 共通パラメータ

- `storage_volume`: ストレージボリュームの名前。署名には、このストレージボリュームの認証情報、エンドポイント、リージョンが使用されます。NULL でない文字列リテラルで、有効化された `S3` タイプのストレージボリュームを指定する必要があります。`builtin_storage_volume` は使用できません。[CREATE STORAGE VOLUME](../../sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md) を参照してください。
- `expiration_time`: 省略可能。URL の有効期間 (秒)。`1` から `3600` までの値を指定します。TINYINT、SMALLINT、INT、BIGINT を指定できます。その他の型は明示的な CAST が必要です。このパラメータを省略した場合、または NULL の場合は、デフォルト値の `3600` が使用されます。

## 署名の規則

- ストレージボリュームに対する USAGE 権限が必要です。
- 署名はローカルで計算されます。これらの関数はオブジェクトにアクセスしないため、オブジェクトが存在するかどうか、認証情報にそのオブジェクトの読み取り権限があるかどうかは確認されません。
- 署名対象の URI は、`<scheme>://<bucket>/<key>` 形式のオブジェクトの絶対 URI である必要があります。サポートされるスキームは BE 設定項目 `s3_compatible_fs_list` で定義され、デフォルトでは `s3`、`s3a`、`s3n`、`oss`、`cos`、`cosn`、`obs`、`ks3`、`tos` です。
- 署名対象のオブジェクト URI に `?` を含めることはできません。含まれている場合、署名付き URL の生成は失敗します。
- ストレージボリュームが一時的な認証情報 (インスタンスプロファイルや Assumed Role など) を使用している場合、URL はその認証情報の有効期限が切れた時点で無効になります。これは `expiration_time` より早くなる可能性があります。
- 引数の値が不正な場合、NULL を返すのではなく、クエリがエラーになります。

## get_presigned_url

`uri` には CHAR 型または VARCHAR 型の定数、列、式を指定できます。署名付き URL を含む VARCHAR 値を返します。`uri` が NULL の場合は NULL を返します。`uri` が `http://` または `https://` の URL の場合、署名付き URL の生成は失敗します。

## get_presigned_file

`file_expr` には [FILE](../../data-types/other-data-types/FILE.md) 式を指定できます。[to_file](to_file.md) や [try_to_file](try_to_file.md) の結果などです。FILE 値を返します。`file_expr` が NULL の場合は NULL を返します。

`uri` のみを署名付き URL に置き換えた新しい FILE を返します。その他のフィールドは変更されません。`uri` が NULL または HTTP/HTTPS URL の場合、FILE をそのまま返します。

## 例

以下の例では、`media_volume` という名前のストレージボリュームを使用します。

### 例 1：デフォルトの有効期間でオブジェクトに署名する

```sql
SELECT get_presigned_url('media_volume', 's3://my-bucket/images/cat.png') AS url;
```

結果は次のようになります。認証情報、日付、署名は環境によって異なります。

```plaintext
https://my-bucket.s3.us-east-1.amazonaws.com/images/cat.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=<access_key>%2F<date>%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=<timestamp>&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature>
```

### 例 2：OSS 上のオブジェクトに署名する

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

### 例 3：FILE 値に署名する

```sql
SELECT get_presigned_file('oss_volume',
    to_file(named_struct('uri', 'oss://zhangjiakou/1.pdf')));
```

`uri` が署名付き URL である新しい FILE を返します。その他のフィールドは変更されません。

戻り値の例：

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
