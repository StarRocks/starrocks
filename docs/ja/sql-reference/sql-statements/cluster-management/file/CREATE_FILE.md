---
displayed_sidebar: docs
---

# CREATE FILE

CREATE FILE ステートメントを実行してファイルを作成できます。ファイルが作成されると、そのファイルはアップロードされ、StarRocks に保存されます。データベース内では、管理者ユーザーのみがファイルを作成および削除でき、データベースにアクセスする権限を持つすべてのユーザーがそのデータベースに属するファイルを使用できます。

:::tip

この操作には、SYSTEM レベルの FILE 権限が必要です。この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 基本概念

**File**: StarRocks に作成および保存されるファイルを指します。ファイルが StarRocks に作成および保存されると、StarRocks はファイルに一意の ID を割り当てます。データベース名、catalog、ファイル名に基づいてファイルを見つけることができます。

## 構文

```SQL
CREATE FILE "file_name" [IN database]
[properties]
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                                      |
| --------------- | -------- | ------------------------------------------------------------ |
| file_name       | Yes      | ファイルの名前。                                              |
| database        | No       | ファイルが属するデータベース。このパラメーターを指定しない場合、現在のセッションでアクセスするデータベースの名前がデフォルトになります。 |
| properties      | Yes      | ファイルのプロパティ。以下の表は、プロパティの設定項目を説明しています。 |

**`properties` の設定項目**

| **設定項目**   | **必須** | **説明**                                                      |
| -------------- | -------- | ------------------------------------------------------------ |
| url            | Yes      | ファイルをダウンロードできる URL。認証されていない HTTP URL のみがサポートされています。ファイルが StarRocks に保存された後は、URL は不要です。 |
| catalog        | Yes      | ファイルが属するカテゴリ。ビジネス要件に基づいて catalog を指定できます。ただし、特定の状況では、このパラメーターを特定の catalog に設定する必要があります。たとえば、Kafka からデータをロードする場合、StarRocks は Kafka データソースの catalog でファイルを検索します。 |
| MD5            | No       | ファイルをチェックするために使用されるメッセージダイジェストアルゴリズム。このパラメーターを指定すると、StarRocks はファイルをダウンロードした後にチェックします。 |

## 例

- kafka という名前のカテゴリに **test.pem** という名前のファイルを作成します。

```SQL
CREATE FILE "test.pem"
PROPERTIES
(
    "url" = "https://starrocks-public.oss-cn-xxxx.aliyuncs.com/key/test.pem",
    "catalog" = "kafka"
);
```

- my_catalog という名前のカテゴリに **client.key** という名前のファイルを作成します。

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