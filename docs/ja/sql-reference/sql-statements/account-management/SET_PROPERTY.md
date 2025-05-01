---
displayed_sidebar: docs
---

# SET PROPERTY

## 説明

### 構文

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

ユーザーに割り当てられたリソースなどを含む、ユーザー属性を設定します。ここでのユーザー属性とは、user_identity ではなくユーザーの属性を意味します。つまり、'jack'@'%' と 'jack'@'192.%' という2人のユーザーが CREATE USER ステートメントを通じて作成された場合、SET PROPERTY ステートメントはユーザー jack に対してのみ使用でき、'jack'@'%' や 'jack'@'192.%' には使用できません。

key:

スーパーユーザー権限:

```plain text
max_user_connections: 最大接続数
resource.cpu_share: CPUリソースの割り当て
```

一般ユーザー権限:

```plain text
quota.normal: 通常レベルでのリソース割り当て
quota.high: 高レベルでのリソース割り当て
quota.low: 低レベルでのリソース割り当て
```

## 例

1. ユーザー jack の最大接続数を1000に変更

    ```SQL
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```

2. ユーザー jack の cpu_share を1000に変更

    ```SQL
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
    ```

3. ユーザー jack の通常レベルの重みを400に変更

    ```SQL
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';
    ```