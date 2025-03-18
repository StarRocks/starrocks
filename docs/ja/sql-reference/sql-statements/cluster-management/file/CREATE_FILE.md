---
displayed_sidebar: docs
---

# CREATE FILE

CREATE FILE ステートメントを実行してファイルを作成できます。ファイルが作成されると、そのファイルは StarRocks にアップロードされ、永続化されます。データベース内では、管理者ユーザーのみがファイルを作成および削除でき、データベースにアクセスする権限を持つすべてのユーザーがそのデータベースに属するファイルを使用できます。

:::tip

この操作には、SYSTEM レベルの FILE 権限が必要です。[GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与できます。

:::

## 基本概念

**File**: StarRocks で作成および保存されるファイルを指します。ファイルが StarRocks に作成および保存されると、StarRocks はファイルに一意の ID を割り当てます。データベース名、catalog、ファイル名に基づいてファイルを見つけることができます。

## 構文

```SQL
CREATE FILE "file_name" [IN database]
[properties]
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | はい          | ファイルの名前。                                        |
| database      | いいえ           | ファイルが属するデータベース。このパラメーターを指定しない場合、現在のセッションでアクセスするデータベースの名前がデフォルトになります。 |
| properties    | はい          | ファイルのプロパティ。以下の表に properties の設定項目を説明します。 |

**`properties` の設定項目**

| **設定項目** | **必須** | **説明**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| url                    | はい          | ファイルをダウンロードできる URL。認証されていない HTTP URL のみがサポートされます。ファイルが StarRocks に保存された後は、URL は不要です。 |
| catalog                | はい          | ファイルが属するカテゴリ。ビジネス要件に基づいて catalog を指定できます。ただし、特定の状況では、このパラメーターを特定の catalog に設定する必要があります。例えば、Kafka からデータをロードする場合、StarRocks は Kafka データソースの catalog でファイルを検索します。 |
| MD5                    | いいえ           | ファイルをチェックするために使用されるメッセージダイジェストアルゴリズム。このパラメーターを指定すると、StarRocks はファイルをダウンロード後にチェックします。 |

## 例

- kafka というカテゴリの下に **test.pem** という名前のファイルを作成します。

```SQL
CREATE FILE "test.pem"
PROPERTIES
(
    "url" = "https://starrocks-public.oss-cn-xxxx.aliyuncs.com/key/test.pem",
    "catalog" = "kafka"
);
```

- my_catalog というカテゴリの下に **client.key** という名前のファイルを作成します。

```SQL
CREATE FILE "client.key"
IN my_database
PROPERTIES
(
    "url" = "http://test.bj.bcebos.com/kafka-key/client.key",
    "catalog" = "my_catalog",
    "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
);
```