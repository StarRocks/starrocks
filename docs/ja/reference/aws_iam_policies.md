---
displayed_sidebar: docs
---

# AWS IAM ポリシー

AWS IAM のポリシーは、特定の AWS リソースに対する一連の権限を宣言します。ポリシーを作成した後、それを IAM ロールまたはユーザーにアタッチする必要があります。これにより、IAM ロールまたはユーザーは、ポリシーで宣言された権限を割り当てられ、指定された AWS リソースにアクセスできます。

StarRocks のさまざまな操作では、異なる AWS リソースへのアクセス権限が必要です。そのため、異なるポリシーを設定する必要があります。

このトピックでは、StarRocks がさまざまなビジネスシナリオで異なる AWS リソースと統合するために必要な IAM ポリシーを提供します。これは、[インスタンスプロファイル、引き受けられたロール、または IAM ユーザーに基づく認証方法](../integrations/authenticate_to_aws_resources.md#preparations)を選択した場合です。

## AWS S3 からのバッチデータロード

S3 バケットからデータをロードしたい場合、次の IAM ポリシーを設定してください。

> **注意**
>
> 次の JSON ポリシーテンプレートの `<bucket_name>` を、データファイルを保存している S3 バケットの名前に置き換えてください。

```SQL
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Sid": "s3",
          "Effect": "Allow",
          "Action": [
              "s3:GetObject"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>/*"
          ]
      },
      {
          "Sid": "s3list",
          "Effect": "Allow",
          "Action": [
              "s3:ListBucket"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>"
          ]
      }
  ]
}
```

## AWS S3 の読み書き

S3 バケットからデータをクエリしたい場合、次の IAM ポリシーを設定してください。

> **注意**
>
> 次の JSON ポリシーテンプレートの `<bucket_name>` を、データファイルを保存している S3 バケットの名前に置き換えてください。

```SQL
{
  "Version": "2012-10-17",
  "Statement": [
      {
          "Sid": "s3",
          "Effect": "Allow",
          "Action": [
              "s3:GetObject", 
              "s3:PutObject"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>/*"
          ]
      },
      {
          "Sid": "s3list",
          "Effect": "Allow",
          "Action": [
              "s3:ListBucket"
          ],
          "Resource": [
              "arn:aws:s3:::<bucket_name>"
          ]
      }
  ]
}
```

## AWS Glue との統合

AWS Glue Data Catalog と統合したい場合、次の IAM ポリシーを設定してください。

```SQL
{
    "Version": "2012-10-17",
    "Statement": [
      {
          "Effect": "Allow",
          "Action": [
              "glue:GetDatabase",
              "glue:GetDatabases",
              "glue:GetPartition",
              "glue:GetPartitions",
              "glue:GetTable",
              "glue:GetTableVersions",
              "glue:GetTables",
              "glue:GetConnection",
              "glue:GetConnections",
              "glue:GetDevEndpoint",
              "glue:GetDevEndpoints",
              "glue:BatchGetPartition"
          ],
          "Resource": [
              "*"
            ]
        }
    ]
}
```