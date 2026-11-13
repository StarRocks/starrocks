---
displayed_sidebar: docs
description: "SHOW AUTHENTICATION displays authentication information of users in the cluster."
---

# SHOW AUTHENTICATION

## 説明

現在のユーザーまたは現在のクラスター内のすべてのユーザーの認証情報を表示します。

:::tip
すべてのユーザーは自分自身の認証情報を表示する権限を持っています。`user_admin` ロールを持つユーザーのみが、すべてのユーザーまたは指定されたユーザーの認証情報を表示できます。
:::

security integration を通じてログインするユーザーは StarRocks 内に作成されません。StarRocks はそのセッション用に仮想ユーザーを作成します。このようなユーザーは、`FOR` を指定しない `SHOW AUTHENTICATION` で自分の認証情報を表示できます。`FOR USERNAME` には StarRocks 内に作成されたユーザーのみ指定できるため、このようなユーザーは指定できず、`SHOW ALL AUTHENTICATION` でも返されません。

## 構文

```SQL
SHOW [ALL] AUTHENTICATION [FOR USERNAME]
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                                                 |
| ---------------- | -------- | ------------------------------------------------------------------------ |
| ALL              | No       | このキーワードが指定されている場合、現在のクラスター内のすべてのユーザーの認証情報が返されます。このキーワードが指定されていない場合、現在のユーザーの認証情報のみが返されます。 |
| USERNAME         | No       | このパラメーターが指定されている場合、指定されたユーザーの認証情報を表示できます。このパラメーターが指定されていない場合、現在のユーザーの認証情報のみを表示できます。 |

## 出力

```SQL
+---------------+----------+-------------+-------------------+
| UserIdentity  | Password | AuthPlugin  | UserForAuthPlugin |
+---------------+----------+-------------+-------------------+
```

| **フィールド**       | **説明**                                                                 |
| -------------------- | ------------------------------------------------------------------------ |
| UserIdentity         | ユーザーの識別情報。                                                     |
| Password             | StarRocks クラスターにログインするためにパスワードが使用されているかどうか。<ul><li>`Yes`: パスワードが使用されています。</li><li>`No`: パスワードは使用されていません。</li></ul>security integration を通じてログインするユーザーの場合、StarRocks はそのパスワードを保存しないため `No` が返されます。 |
| AuthPlugin           | 認証に使用されるインターフェース。 有効な値は `MYSQL_NATIVE_PASSWORD` と `AUTHENTICATION_LDAP_SIMPLE` です。インターフェースが使用されていない場合、`NULL` が返されます。security integration を通じてログインするユーザーの場合、その security integration の名前が返されます。 |
| UserForAuthPlugin    | LDAP または Kerberos 認証を使用しているユーザーの名前。認証が使用されていない場合、`NULL` が返されます。 |

## 例

例 1: 現在のユーザーの認証情報を表示します。

```Plain
SHOW AUTHENTICATION;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | No       | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```

例 2: 現在のクラスター内のすべてのユーザーの認証情報を表示します。

```Plain
SHOW ALL AUTHENTICATION;
+---------------+----------+----------------------------+-------------------+
| UserIdentity  | Password | AuthPlugin                 | UserForAuthPlugin |
+---------------+----------+----------------------------+-------------------+
| 'root'@'%'    | Yes      | NULL                       | NULL              |
| 'chelsea'@'%' | No       | AUTHENTICATION_LDAP_SIMPLE | NULL              |
+---------------+----------+----------------------------+-------------------+
```

例 3: 指定されたユーザーの認証情報を表示します。

```Plain
SHOW AUTHENTICATION FOR root;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | Yes      | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```

例 4: security integration `ldap_si` を通じてログインした現在のユーザーの認証情報を表示します。

```Plain
SHOW AUTHENTICATION;
+------------------------+----------+------------+-------------------+
| UserIdentity           | Password | AuthPlugin | UserForAuthPlugin |
+------------------------+----------+------------+-------------------+
| 'alice'@'192.168.1.10' | No       | ldap_si    | NULL              |
+------------------------+----------+------------+-------------------+
```
