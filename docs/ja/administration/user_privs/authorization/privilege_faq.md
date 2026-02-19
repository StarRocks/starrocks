---
displayed_sidebar: docs
sidebar_position: 50
---

# Privilege FAQ

## 必要なロールがユーザーに割り当てられているにもかかわらず、「no permission」というエラーメッセージが報告されるのはなぜですか？

このエラーは、ロールがアクティブ化されていない場合に発生することがあります。`select current_role();` を実行して、現在のセッションでユーザーにアクティブ化されているロールを確認できます。必要なロールがアクティブ化されていない場合は、[SET ROLE](../../../sql-reference/sql-statements/account-management/SET_ROLE.md) を実行してこのロールをアクティブ化し、このロールを使用して操作を行ってください。

ログイン時にロールを自動的にアクティブ化したい場合、`user_admin` ロールは [SET DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) または [ALTER USER DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) を実行して、各ユーザーのデフォルトロールを設定できます。デフォルトロールが設定されると、ユーザーがログインしたときに自動的にアクティブ化されます。

すべてのユーザーに割り当てられたすべてのロールをログイン時に自動的にアクティブ化したい場合は、次のコマンドを実行できます。この操作には、システムレベルでの OPERATE 権限が必要です。

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

しかし、潜在的なリスクを防ぐために、権限が制限されたデフォルトロールを設定する「最小権限」の原則に従うことをお勧めします。例えば：

- 一般ユーザーは、SELECT 権限のみを持つ `read_only` ロールをデフォルトロールとして設定し、ALTER、DROP、INSERT などの権限を持つロールをデフォルトロールとして設定しないようにします。
- 管理者は、`db_admin` ロールをデフォルトロールとして設定し、ノードの追加や削除の権限を持つ `node_admin` ロールをデフォルトロールとして設定しないようにします。

このアプローチは、ユーザーに適切な権限を持つロールを割り当てるのに役立ち、意図しない操作のリスクを軽減します。

[GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) を実行して、必要な権限やロールをユーザーに割り当てることができます。

## データベース内のすべてのテーブルに対する権限をユーザーに付与しました（`GRANT ALL ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;`）が、ユーザーがデータベース内でテーブルを作成できません。なぜですか？

データベース内でテーブルを作成するには、データベースレベルの CREATE TABLE 権限が必要です。この権限をユーザーに付与する必要があります。

```SQL
GRANT CREATE TABLE ON DATABASE <db_name> TO USER <user_identity>;
```

## データベースに対するすべての権限をユーザーに付与しました（`GRANT ALL ON DATABASE <db_name> TO USER <user_identity>;`）が、ユーザーがこのデータベースで `SHOW TABLES;` を実行しても何も返されません。なぜですか？

`SHOW TABLES;` は、ユーザーが何らかの権限を持っているテーブルのみを返します。ユーザーがテーブルに対して権限を持っていない場合、そのテーブルは返されません。このデータベース内のすべてのテーブルに対して（例えば SELECT を使用して）ユーザーに何らかの権限を付与できます。

```SQL
GRANT SELECT ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;
```

上記のステートメントは、v3.0 より前のバージョンで使用されていた `GRANT select_priv ON db.* TO <user_identity>;` と同等です。

## StarRocks Web Console `http://<fe_ip>:<fe_http_port>` にアクセスするために必要な権限は何ですか？

ユーザーは `cluster_admin` ロールを持っている必要があります。

## StarRocks v3.0 の前後で権限保持メカニズムはどのように変わりましたか？

v3.0 より前は、ユーザーがテーブルに対して権限を付与された後、そのテーブルが削除され再作成されても権限は保持されていました。v3.0 以降、テーブルが削除され再作成された場合、権限は保持されなくなります。

## StarRocks でユーザーと付与された権限をどのように照会しますか？

システムビュー `sys.grants_to_users` を照会するか、SHOW USERS を実行して完全なユーザーリストを取得し、その後、`SHOW GRANTS FOR <user_identity>` を使用して各ユーザーを個別に照会できます。

## 多数のユーザーとテーブルの権限メタデータに関するシステムビューを照会する際の FE リソースへの影響は何ですか？

ユーザーやテーブルの数が非常に多い場合、システムビュー `sys.grants_to_users`、`sys.grants_to_roles`、および `sys.role_edges` のクエリは時間がかかることがあります。これらのビューはリアルタイムで計算され、FE リソースの一部を消費します。したがって、大規模に頻繁にこのような操作を実行することは推奨されません。

## catalog を再作成すると権限が失われますか？権限をどのようにバックアップおよび復元しますか？

はい。catalog を再作成すると、その関連する権限が失われます。まずすべてのユーザー権限をバックアップし、catalog を再作成した後に復元する必要があります。

## 自動権限移行をサポートするツールはありますか？

現時点ではありません。ユーザーは各ユーザーに対して SHOW GRANTS を使用して手動で権限をバックアップおよび復元する必要があります。

## KILL コマンドの使用に制限はありますか？ユーザー自身のクエリのみを終了させるように制限できますか？

はい。KILL コマンドは現在 OPERATE 権限を必要とし、ユーザーは自分自身が開始したクエリのみを終了させることができます。

## テーブルの名前変更や削除後に付与された権限が変わるのはなぜですか？システムは古い権限を保持しながら、名前変更されたテーブルに対する権限を追加できますか？

内部テーブルの場合、権限はテーブル名ではなくテーブル ID に結び付けられています。これにより、テーブル名が任意に変更される可能性があるため、データのセキュリティが確保されます。権限がテーブル名に従うと、データ漏洩が発生する可能性があります。同様に、テーブルが削除されると、そのオブジェクトが存在しなくなるため、権限も削除されます。

外部テーブルの場合、過去のバージョンでは内部テーブルと同様に動作していました。しかし、外部テーブルのメタデータは StarRocks によって管理されていないため、遅延や権限の喪失が発生する可能性があります。これに対処するため、将来のバージョンでは外部テーブルに対してテーブル名に基づく権限管理が使用され、期待される動作に一致します。

## ユーザー権限をどのようにバックアップしますか？

以下は、クラスタ内のユーザー権限情報をバックアップするためのサンプルスクリプトです。

```Bash
#!/bin/bash

# MySQL connection info
HOST=""
PORT="9030"
USER="root"
PASSWORD=""  
OUTPUT_FILE="user_privileges.txt"

# Clear output file
> $OUTPUT_FILE

# Get user list
users=$(mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW USERS;" | sed -e '1d' -e '/^+/d')

# Loop through users and get privileges
for user in $users; do
    echo "Privileges for $user:" >> $OUTPUT_FILE
    mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW GRANTS FOR $user;" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
done

echo "All user privileges have been written to $OUTPUT_FILE"
```

## 通常の関数に USAGE を付与すると「Unexpected input 'IN', the most similar input is {'TO'}」というエラーが発生するのはなぜですか？関数に対する権限を付与する正しい方法は何ですか？

通常の関数は IN ALL DATABASES を使用して付与することはできません。現在のデータベース内でのみ付与できます。一方、グローバル関数は ALL DATABASES スケールで付与されます。