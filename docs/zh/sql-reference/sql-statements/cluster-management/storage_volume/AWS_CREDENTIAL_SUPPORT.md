---
displayed_sidebar: docs
description: "对比 FE 运行时与基于 StarOS 的存储卷对 AWS 凭证的支持情况。"
sidebar_position: 10
---

# 存储卷 AWS 凭证支持情况

StarRocks FE 使用 `AwsCloudCredential` 为存储卷及其他 AWS 使用方配置 AWS 认证。存储卷还会将凭证序列化至
StarOS。FE 运行时支持的部分组合目前无法完整保存在存储卷中。

## 支持矩阵

| 认证方式 | 必需属性 | FE 运行时 | 存储卷 | 说明 |
|---|---|:---:|:---:|---|
| AWS SDK 默认凭证链 | `aws.s3.use_aws_sdk_default_behavior=true` | 支持 | 支持 | 使用 AWS SDK 默认凭证提供链。 |
| 默认凭证链后执行 AssumeRole | `aws.s3.use_aws_sdk_default_behavior=true`<br />`aws.s3.iam_role_arn=<arn>`<br />可选：`aws.s3.external_id` | 支持 | 不支持 | 存储卷仅保留默认凭证链的选择。 |
| Instance Profile | `aws.s3.use_instance_profile=true` | 支持 | 支持 | 使用 EC2 Instance Profile 凭证。 |
| Instance Profile 后执行 AssumeRole | `aws.s3.use_instance_profile=true`<br />`aws.s3.iam_role_arn=<arn>`<br />可选：`aws.s3.external_id` | 支持 | 支持 | Role ARN 和 External ID 均会保留。 |
| Web Identity Token 文件 | `aws.s3.use_web_identity_token_file=true`<br />Worker 环境变量：`AWS_WEB_IDENTITY_TOKEN_FILE` 和 `AWS_ROLE_ARN` | 支持 | 支持 | Token 文件和第一跳 Role 由各 Worker 解析。 |
| Web Identity 后执行第二次 AssumeRole | `aws.s3.use_web_identity_token_file=true`<br />`aws.s3.iam_role_arn=<second_hop_arn>`<br />可选：`aws.s3.external_id`<br />Worker 环境变量：`AWS_WEB_IDENTITY_TOKEN_FILE` 和 `AWS_ROLE_ARN` | 支持 | 支持 | 配置的 Role ARN 表示第二次 STS 跳转。 |
| 静态 Access Key 和 Secret Key | `aws.s3.access_key=<access_key>`<br />`aws.s3.secret_key=<secret_key>` | 支持 | 支持 | Access Key 和 Secret Key 均会保留。 |
| 静态临时凭证 | Access Key 和 Secret Key 属性<br />`aws.s3.session_token=<token>` | 支持 | 不支持 | Session Token 不会保存在存储卷中。 |
| Access Key 和 Secret Key 后执行 AssumeRole | Access Key 和 Secret Key 属性<br />`aws.s3.iam_role_arn=<arn>`<br />可选：`aws.s3.external_id` | 支持 | 不支持 | AssumeRole 配置不会保存在存储卷中。 |
| 自定义 STS Region 或 Endpoint | 有效的基础凭证<br />`aws.s3.sts.region=<region>` 和/或 `aws.s3.sts.endpoint=<endpoint>`<br />`aws.s3.iam_role_arn=<arn>` | 支持 | 不支持 | 自定义 STS 配置不会保存在存储卷中。 |
| 仅配置 Role ARN，未配置基础凭证 | 仅配置 `aws.s3.iam_role_arn=<arn>` | 不支持 | 不支持 | AssumeRole 需要有效的基础凭证提供方。 |

请勿使用标记为**不支持**的组合配置存储卷。尽管 FE 可以构造对应的凭证提供方，但存储卷序列化至 StarOS
时无法保留完整配置。

## 凭证解析顺序

如果配置了多种基础认证方式，FE 按以下顺序选择第一个匹配项：

```text
AWS SDK 默认凭证链 > Instance Profile > Web Identity > Access Key 和 Secret Key
```

选择基础凭证提供方后，如果 `aws.s3.iam_role_arn` 非空，FE 将执行 STS AssumeRole 操作。

## 切换认证方式

`ALTER STORAGE VOLUME` 会将您指定的属性合并到存储卷现有属性中，而非替换。结合上述解析顺序，仅设置新认证方式
属性的语句可能执行成功，但存储卷实际使用的凭证并未改变。

将存储卷从一种认证方式切换为另一种时：

- 显式设置全部 `use_*` 属性，且其中仅有一个为 `true`。
- 除非新认证方式同样需要执行 AssumeRole，否则请清空 `aws.s3.iam_role_arn` 和 `aws.s3.external_id`。这两项不是
  `use_*` 属性，因此会在合并后保留，并被应用到新的认证方式上。

### 无法切换认证方式的语句

以下语句均可执行成功，但存储卷仍保留原有凭证，且您设置的属性不会被保存。

```SQL
-- 存储卷当前使用 Web Identity Token 文件。
-- 无效果：Web Identity 的优先级高于 Access Key 和 Secret Key。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- 存储卷当前使用 Instance Profile。
-- 无效果：Instance Profile 的优先级高于 Web Identity。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_web_identity_token_file" = "true"
);
```

### 可以切换认证方式的语句

```SQL
-- Web Identity 切换为 Access Key 和 Secret Key。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "false",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- Instance Profile 切换为 Web Identity，并清除原有 Role。
ALTER STORAGE VOLUME <storage_volume_name> SET (
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.use_web_identity_token_file" = "true",
    "aws.s3.iam_role_arn" = "",
    "aws.s3.external_id" = ""
);
```

切换为优先级更高的认证方式时，无需禁用当前认证方式也能生效，因为新方式会被优先匹配。例如，使用 Access Key 和
Secret Key 的存储卷，仅将 `aws.s3.use_web_identity_token_file` 设置为 `true` 即可切换为 Web Identity。显式设置
全部 `use_*` 属性在两个方向上均有效，且无需事先了解当前生效的认证方式。

:::caution
上一种认证方式遗留的 Role ARN 会被应用到新的认证方式上。如果存储卷此前使用 Web Identity 并执行第二次
AssumeRole，切换为 Instance Profile 后，遗留的 `aws.s3.iam_role_arn` 将成为 Instance Profile 所要 Assume 的
Role。请在同一条语句中清空 `aws.s3.iam_role_arn` 和 `aws.s3.external_id` 以避免该问题。
:::

有关存储卷配置示例，请参阅 [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md#认证信息)。
