---
displayed_sidebar: docs
description: "FE ランタイムと StarOS ベースのストレージボリュームにおける AWS 認証情報のサポートを比較します。"
sidebar_position: 10
---

# ストレージボリュームの AWS 認証情報サポート

StarRocks FE は `AwsCloudCredential` を使用して、ストレージボリュームやその他の AWS コンシューマーの AWS
認証を設定します。ストレージボリュームはさらに認証情報を StarOS にシリアライズします。FE ランタイムで
サポートされる一部の組み合わせは、現在ストレージボリュームに完全には保存できません。

## サポートマトリックス

| 認証方式 | 必須プロパティ | FE ランタイム | ストレージボリューム | 説明 |
|---|---|:---:|:---:|---|
| AWS SDK デフォルト認証情報チェーン | `aws.s3.use_aws_sdk_default_behavior=true` | サポート | サポート | AWS SDK のデフォルト認証情報プロバイダーチェーンを使用します。 |
| デフォルト認証情報チェーンの後に AssumeRole | `aws.s3.use_aws_sdk_default_behavior=true`<br />`aws.s3.iam_role_arn=<arn>`<br />任意：`aws.s3.external_id` | サポート | 非サポート | ストレージボリュームにはデフォルトチェーンの選択のみが保存されます。 |
| Instance Profile | `aws.s3.use_instance_profile=true` | サポート | サポート | EC2 Instance Profile 認証情報を使用します。 |
| Instance Profile の後に AssumeRole | `aws.s3.use_instance_profile=true`<br />`aws.s3.iam_role_arn=<arn>`<br />任意：`aws.s3.external_id` | サポート | サポート | Role ARN と External ID が保存されます。 |
| Web Identity Token ファイル | `aws.s3.use_web_identity_token_file=true`<br />ワーカー環境：`AWS_WEB_IDENTITY_TOKEN_FILE` と `AWS_ROLE_ARN` | サポート | サポート | Token ファイルと最初の Role は各ワーカーで解決されます。 |
| Web Identity の後に 2 回目の AssumeRole | `aws.s3.use_web_identity_token_file=true`<br />`aws.s3.iam_role_arn=<second_hop_arn>`<br />任意：`aws.s3.external_id`<br />ワーカー環境：`AWS_WEB_IDENTITY_TOKEN_FILE` と `AWS_ROLE_ARN` | サポート | サポート | 設定した Role ARN は 2 回目の STS ホップを表します。 |
| 静的 Access Key と Secret Key | `aws.s3.access_key=<access_key>`<br />`aws.s3.secret_key=<secret_key>` | サポート | サポート | Access Key と Secret Key が保存されます。 |
| 静的セッション認証情報 | Access Key と Secret Key のプロパティ<br />`aws.s3.session_token=<token>` | サポート | 非サポート | Session Token はストレージボリュームに保存されません。 |
| Access Key と Secret Key の後に AssumeRole | Access Key と Secret Key のプロパティ<br />`aws.s3.iam_role_arn=<arn>`<br />任意：`aws.s3.external_id` | サポート | 非サポート | AssumeRole 設定はストレージボリュームに保存されません。 |
| カスタム STS Region または Endpoint | 有効なベース認証情報<br />`aws.s3.sts.region=<region>` および/または `aws.s3.sts.endpoint=<endpoint>`<br />`aws.s3.iam_role_arn=<arn>` | サポート | 非サポート | カスタム STS 設定はストレージボリュームに保存されません。 |
| ベース認証情報なしで Role ARN のみを設定 | `aws.s3.iam_role_arn=<arn>` のみ | 非サポート | 非サポート | AssumeRole には有効なベース認証情報プロバイダーが必要です。 |

**非サポート**と示された組み合わせでストレージボリュームを設定しないでください。FE は対応する認証情報
プロバイダーを構築できますが、ストレージボリュームを StarOS にシリアライズすると完全な設定が保持されません。

## 認証情報の解決順序

複数のベース認証方式を設定した場合、FE は次の順序で最初に一致する方式を選択します。

```text
AWS SDK デフォルト認証情報チェーン > Instance Profile > Web Identity > Access Key と Secret Key
```

ベース認証情報プロバイダーを選択した後、`aws.s3.iam_role_arn` が空でなければ、FE は STS AssumeRole
操作を実行します。

## 認証方式の切り替え

`ALTER STORAGE VOLUME` は、指定したプロパティをストレージボリュームの既存のプロパティに置き換えるのではなく
マージします。上記の解決順序と組み合わさるため、新しい認証方式のプロパティのみを設定した文は成功しても、
ストレージボリュームが実際に使用する認証情報は変わらないことがあります。

ストレージボリュームの認証方式を切り替えるには、次のようにします。

- すべての `use_*` プロパティを明示的に設定し、そのうち 1 つだけを `true` にします。
- 新しい認証方式でも AssumeRole が必要な場合を除き、`aws.s3.iam_role_arn` と `aws.s3.external_id` をクリア
  します。これらは `use_*` プロパティではないためマージ後も残り、新しい認証方式に適用されます。

### 認証方式が切り替わらない文

次の文はいずれも成功しますが、ストレージボリュームは以前の認証情報を保持し、指定したプロパティは保存されません。

```SQL
-- ストレージボリュームは現在 Web Identity Token ファイルを使用しています。
-- 効果なし: Web Identity は Access Key と Secret Key より優先されます。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- ストレージボリュームは現在 Instance Profile を使用しています。
-- 効果なし: Instance Profile は Web Identity より優先されます。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_web_identity_token_file" = "true"
);
```

### 認証方式が切り替わる文

```SQL
-- Web Identity から Access Key と Secret Key へ。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- Instance Profile から Web Identity へ。以前の Role は削除します。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "true",
    "aws.s3.iam_role_arn" = "",
    "aws.s3.external_id" = ""
);
```

現在の方式より優先度の高い方式へ切り替える場合は、現在の方式を無効にしなくても切り替わります。新しい方式が
先に一致するためです。たとえば、Access Key と Secret Key を使用するストレージボリュームは、
`aws.s3.use_web_identity_token_file` を `true` に設定するだけで Web Identity に切り替わります。すべての
`use_*` プロパティを明示的に設定する方法は双方向で有効であり、現在有効な方式を知る必要がありません。

:::caution
以前の認証方式から残った Role ARN は、新しい認証方式に適用されます。ストレージボリュームが Web Identity と
2 回目の AssumeRole を使用していた場合、Instance Profile に切り替えると、残った `aws.s3.iam_role_arn` が
Instance Profile が Assume する Role になります。これを避けるには、同じ文の中で `aws.s3.iam_role_arn` と
`aws.s3.external_id` をクリアしてください。
:::

ストレージボリュームの設定例については、[CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md#認証情報) を参照してください。
