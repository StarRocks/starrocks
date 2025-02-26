---
displayed_sidebar: docs
---

# is_role_in_session

ロール（またはネストされたロール）が現在のセッションでアクティブかどうかを確認します。

この関数は v3.1.4 以降でサポートされています。

## 構文

```Haskell
BOOLEAN is_role_in_session(VARCHAR role_name);
```

## パラメータ

`role_name`: 確認したいロール（ネストされたロールも可）。サポートされているデータ型は VARCHAR です。

## 戻り値

BOOLEAN 値を返します。`1` はロールが現在のセッションでアクティブであることを示します。`0` はその逆を示します。

## 例

1. ロールとユーザーを作成します。

   ```sql
   -- 3 つのロールを作成します。
   create role r1;
   create role r2;
   create role r3;

   -- ユーザー u1 を作成します。
   create user u1;

   -- ロール r2 と r3 を r1 に渡し、r1 をユーザー u1 に付与します。この方法で、ユーザー u1 は 3 つのロールを持ちます: r1, r2, および r3。
   grant r3 to role r2;
   grant r2 to role r1;
   grant r1 to user u1;

   -- ユーザー u1 に切り替え、u1 として操作を行います。
   execute as u1 with no revert;
   ```

2. `r1` がアクティブかどうかを確認します。結果はこのロールがアクティブでないことを示しています。

   ```plaintext
   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        0 |
   +--------------------------+
   ```

3. [SET ROLE](../../sql-statements/account-management/SET_ROLE.md) コマンドを実行して `r1` をアクティブにし、`is_role_in_session` を使用してロールがアクティブかどうかを確認します。結果は `r1` がアクティブであり、`r1` にネストされたロール `r2` と `r3` もアクティブであることを示しています。

   ```sql
   set role "r1";

   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r2");
   +--------------------------+
   | is_role_in_session('r2') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r3");
   +--------------------------+
   | is_role_in_session('r3') |
   +--------------------------+
   |                        1 |
   +--------------------------+
   ```