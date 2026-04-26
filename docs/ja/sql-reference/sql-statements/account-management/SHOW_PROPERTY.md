---
displayed_sidebar: docs
---

# SHOW PROPERTY

## 説明

SHOW PROPERTY は、最大接続数、デフォルトカタログ、デフォルトデータベースなどのユーザープロパティを表示します。

:::tip
現在のユーザーは自分のプロパティを表示できます。`user_admin` ロールを持つユーザーのみが他のユーザーのプロパティを表示できます。

:::

:::info
`database` や `catalog` などのプロパティを設定するには、`SET PROPERTIES` を指定した [ALTER USER](./ALTER_USER.md) コマンドを使用してください。
`max_user_connections` については、`SET PROPERTY` 構文を使用できます。
:::

## 構文

```SQL
SHOW PROPERTY [FOR 'user_name'] [LIKE '<property_name>']
```

## パラメータ

| **パラメータ**       | **必須**    | **説明**                                                                      |
| -------------------- | ------------ | ---------------------------------------------------------------------------- |
| user_name            | いいえ       | ユーザー名。指定しない場合、現在のユーザーのプロパティが表示されます。         |
| property_name        | いいえ       | ユーザープロパティ名。                                                        |

## 例

例 1: 現在のユーザーのプロパティを表示します。

```Plain
SHOW PROPERTY;

+----------------------+-----------------+
| Key                  | Value           |
+----------------------+-----------------+
| max_user_connections | 1024            |
| catalog              | default_catalog |
| database             |                 |
+----------------------+-----------------+
```

例 2: ユーザー `jack` のプロパティを表示します。

```SQL
SHOW PROPERTY FOR 'jack';
```

```Plain
+----------------------+------------------+
| Key                  | Value            |
+----------------------+------------------+
| max_user_connections | 100              |
| catalog              | default_catalog  |
| database             | sales_db         |
+----------------------+------------------+
```

例 3: `LIKE` を使用してプロパティをフィルタリングします。

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```

```Plain
+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 100   |
+----------------------+-------+
```

## 参照

[ALTER USER](./ALTER_USER.md): ユーザーのプロパティを設定します。