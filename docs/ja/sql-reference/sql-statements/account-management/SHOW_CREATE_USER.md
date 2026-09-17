---
displayed_sidebar: docs
description: "SHOW CREATE USER returns the CREATE USER statement that reproduces a user."
---

# SHOW CREATE USER

## 説明

指定したユーザーを再作成できる `CREATE USER` 文を返します。認証方式、デフォルトロール、アカウントの状態、およびユーザープロパティが含まれます。

返されるのはユーザーの現在の状態です。ユーザー作成後に `ALTER USER` や `SET PASSWORD` で行った変更も反映されます。

:::tip
すべてのユーザーは自分自身に対してこの文を実行できます。他のユーザーを表示するには、`user_admin` ロールまたは SYSTEM に対する GRANT 権限が必要です。

パスワードダイジェストは、SYSTEM に対する SHOW SECRET 権限を持つユーザーにのみ返されます。権限がない場合は `<secret>` としてマスクされます。これは自分自身のユーザーを表示する場合にも適用されます。

SHOW SECRET はダイジェストを既定で与えないだけであり、管理者を制限する境界ではありません。SYSTEM に対する GRANT 権限を持つユーザー（`user_admin` ロールを含む）は、自分自身に SHOW SECRET を付与できます。
:::

## 構文

```SQL
SHOW CREATE USER { <user_identity> | CURRENT_USER() }
```

## パラメーター

| **パラメーター** | **必須** | **説明**                                                     |
| ---------------- | -------- | ------------------------------------------------------------ |
| user_identity    | No       | 表示するユーザー。`'user_name'@'host'` の形式で指定します。   |
| CURRENT_USER()   | No       | 現在のユーザーを表示します。自分自身のユーザー識別子を指定した場合と同じです。 |

## 出力

```SQL
+------+-------------+
| User | Create User |
+------+-------------+
```

| **フィールド** | **説明**                                       |
| -------------- | ---------------------------------------------- |
| User           | ユーザー識別子。                               |
| Create User    | そのユーザーを再作成できる `CREATE USER` 文。   |

各句はデフォルト値と異なる場合のみ出力されます。

| 句                | 出力される条件                                               |
| ----------------- | ------------------------------------------------------------ |
| `IDENTIFIED WITH` | ユーザーにパスワードが設定されているか、`mysql_native_password` 以外の認証プラグインを使用している場合。 |
| `DEFAULT ROLE`    | ユーザーにデフォルトロールが 1 つ以上設定されている場合。    |
| `EXPIRE_PASSWORD` | パスワードが期限切れとしてマークされている場合。             |
| `LOCK`            | アカウントがロックされている場合。                           |
| `PROPERTIES`      | ユーザープロパティのいずれかがデフォルト値と異なる場合。     |

:::note
出力される文には 3 つの制限があります。

- マスクされた文をそのまま再実行してはいけません。`<secret>` はパスワードではありません。`mysql_native_password` では不正なダイジェストとして拒否されますが、他の認証プラグインでは認証文字列としてそのまま受け入れられ、誰もログインできないアカウントが作成される可能性があります。SHOW SECRET 権限を持つユーザーで取得し直してください。
- ユーザープロパティはユーザー識別子単位ではなくユーザー名単位で保存されます。そのため `'jack'@'192.168.%'` と `'jack'@'10.1.%'` は同じ `PROPERTIES` 句を返します。
- `PASSWORD_POLICY` プロパティまたはクラスター全体のポリシーによってユーザーにパスワードポリシーが適用されている場合、出力された文はそのままでは再実行できません。`IDENTIFIED WITH ... AS` はパスワードダイジェストを渡しますが、パスワードポリシーは平文パスワードしか検証できないため、`CREATE USER` は `Because the Password Policy is in effect, you cannot use a hashed password.` で拒否します。そのようなユーザーは平文パスワードで作り直してください。
:::

## 例

デフォルトロールを持つユーザーを作成します。

```SQL
CREATE USER 'jack'@'%' IDENTIFIED BY '123456' DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100");
```

例 1: SHOW SECRET 権限を持つユーザーが表示した場合。

```Plain
SHOW CREATE USER 'jack'@'%'\G
*************************** 1. row ***************************
       User: 'jack'@'%'
Create User: CREATE USER 'jack'@'%'
IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100")
1 row in set (0.00 sec)
```

例 2: SHOW SECRET 権限を持たないユーザーが同じユーザーを表示した場合。

```Plain
SHOW CREATE USER 'jack'@'%'\G
*************************** 1. row ***************************
       User: 'jack'@'%'
Create User: CREATE USER 'jack'@'%'
IDENTIFIED WITH mysql_native_password AS '<secret>'
DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100")
1 row in set (0.00 sec)
```

例 3: 現在のユーザーを表示します。パスワード、デフォルトロールがなく、プロパティがすべてデフォルト値のユーザーは 1 行の文になります。

```Plain
SHOW CREATE USER CURRENT_USER()\G
*************************** 1. row ***************************
       User: 'tom'@'%'
Create User: CREATE USER 'tom'@'%'
1 row in set (0.00 sec)
```

例 4: SHOW SECRET 権限を付与して、パスワードダイジェストを表示できるようにします。

```SQL
GRANT SHOW SECRET ON SYSTEM TO USER 'jack'@'%';
```

## 関連ドキュメント

- [CREATE USER](CREATE_USER.md)
- [ALTER USER](ALTER_USER.md)
- [SHOW AUTHENTICATION](SHOW_AUTHENTICATION.md)
- [GRANT](GRANT.md)
