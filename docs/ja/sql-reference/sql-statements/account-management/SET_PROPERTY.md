---
displayed_sidebar: docs
---

# SET PROPERTY

## 説明

ユーザーのプロパティを設定します。現在、このコマンドで設定できるのはユーザー接続数 (`max_user_connections`) のみです。

:::tip

- ユーザープロパティはユーザーのプロパティを意味し、user_identity ではありません。例えば、'jack'@'%' と 'jack'@'192.%' の2つのユーザーが CREATE USER を使用して作成された場合、SET PROPERTY ステートメントはユーザー `jack` にのみ適用され、'jack'@'%' や 'jack'@'192.%' には適用されません。
- この操作を行うには、`user_admin` ロールを持つユーザーのみが可能です。

:::

### 構文

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

## 例

1. ユーザー `jack` の最大接続数を `1000` に変更します。

    ```SQL
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```