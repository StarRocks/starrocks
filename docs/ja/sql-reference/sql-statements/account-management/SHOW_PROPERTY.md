---
displayed_sidebar: docs
---

# SHOW PROPERTY

## 説明

ユーザーのプロパティを表示します。現在、このコマンドで表示できるのは接続の最大数のみです。

:::tip
現在のユーザーは自分のプロパティを表示できます。`user_admin` ロールを持つユーザーのみが他のユーザーのプロパティを表示できます。

:::

## 構文

```SQL
SHOW PROPERTY [FOR 'user_name'] [LIKE 'max_user_connections']
```

## パラメーター

| **パラメーター**              | **必須** | **説明**                                    |
| -------------------- | -------- | ----------------------------------------- |
| user_name            | いいえ       | ユーザー名。指定しない場合、現在のユーザーのプロパティが表示されます。 |
| max_user_connections | いいえ       | ユーザーの接続の最大数。      |

## 例

例 1: 現在のユーザーの接続の最大数を表示します。

```Plain
SHOW PROPERTY;

+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 10000 |
+----------------------+-------+
```

例 2: ユーザー `jack` の接続の最大数を表示します。

```SQL
SHOW PROPERTY FOR 'jack';
```

または

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

## 関連項目

[SET PROPERTY](./SET_PROPERTY.md): ユーザーの接続の最大数を設定します。