---
displayed_sidebar: docs
---

# EXECUTE AS

## 説明

ユーザーを偽装する権限 (IMPERSONATE) を取得した後、EXECUTE AS ステートメントを使用して、現在のセッションの実行コンテキストをそのユーザーに切り替えることができます。

このコマンドは v2.4 からサポートされています。

## 構文

```SQL
EXECUTE AS user WITH NO REVERT
```

## パラメーター

`user`: ユーザーは既に存在している必要があります。

## 使用上の注意

- 現在ログインしているユーザー (EXECUTE AS ステートメントを呼び出すユーザー) は、他のユーザーを偽装する権限を付与されている必要があります。詳細については、[GRANT](../account-management/GRANT.md) を参照してください。
- EXECUTE AS ステートメントには WITH NO REVERT 句を含める必要があります。これは、現在のセッションが終了する前に、元のログインユーザーに実行コンテキストを戻すことができないことを意味します。

## 例

現在のセッションの実行コンテキストをユーザー `test2` に切り替えます。

```SQL
EXECUTE AS test2 WITH NO REVERT;
```

切り替えが成功した後、`select current_user()` コマンドを実行して現在のユーザーを取得できます。

```SQL
select current_user();
+-----------------------------+
| CURRENT_USER()              |
+-----------------------------+
| 'default_cluster:test2'@'%' |
+-----------------------------+
```