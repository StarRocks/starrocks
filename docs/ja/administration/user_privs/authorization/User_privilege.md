---
displayed_sidebar: docs
sidebar_position: 30
---

# ユーザー権限の管理

import UserPrivilegeCase from '../../../_assets/commonMarkdown/userPrivilegeCase.md'
import GrantToGroup from '../../../_assets/commonMarkdown/grant_to_group.mdx'

このトピックでは、StarRocks におけるユーザー、ロール、および権限の管理方法について説明します。

StarRocks は、ロールベースのアクセス制御 (RBAC) とアイデンティティベースのアクセス制御 (IBAC) の両方を使用して StarRocks クラスター内の権限を管理し、クラスター管理者が異なる粒度レベルでクラスター内の権限を簡単に制限できるようにします。

StarRocks クラスター内では、権限はユーザーまたはロールに付与できます。ロールは、必要に応じてクラスター内のユーザーまたは他のロールに割り当てることができる権限の集合です。ユーザーには 1 つ以上のロールを付与でき、それによって異なるオブジェクトに対する権限が決まります。

## 権限とロール情報の表示

システム定義のロール `user_admin` を持つユーザーは、StarRocks クラスター内のすべてのユーザーおよびロール情報を表示できます。

### 権限情報の表示

ユーザーまたはロールに付与された権限を [SHOW GRANTS](../../../sql-reference/sql-statements/account-management/SHOW_GRANTS.md) を使用して表示できます。

- 現在のユーザーの権限を表示する。

  ```SQL
  SHOW GRANTS;
  ```

  > **注意**
  >
  > すべてのユーザーは、自分の権限を表示することができます。

- 特定のユーザーの権限を表示する。

  次の例は、ユーザー `jack` の権限を示しています。

  ```SQL
  SHOW GRANTS FOR jack@'172.10.1.10';
  ```

- 特定のロールの権限を表示する。

  次の例は、ロール `example_role` の権限を示しています。

  ```SQL
  SHOW GRANTS FOR ROLE example_role;
  ```

### ロールの表示

StarRocks クラスター内のすべてのロールを [SHOW ROLES](../../../sql-reference/sql-statements/account-management/SHOW_ROLES.md) を使用して表示できます。

```SQL
SHOW ROLES;
```

## ロールの管理

システム定義のロール `user_admin` を持つユーザーは、StarRocks でロールを作成、付与、取り消し、または削除できます。

### ロールの作成

[CREATE ROLE](../../../sql-reference/sql-statements/account-management/CREATE_ROLE.md) を使用してロールを作成できます。デフォルトでは、ユーザーは最大 64 のロールを持つことができます。この設定は、FE ダイナミックパラメータ `privilege_max_total_roles_per_user` を使用して調整できます。ロールは最大 16 の継承レベルを持つことができます。この設定は、FE ダイナミックパラメータ `privilege_max_role_depth` を使用して調整できます。

次の例は、ロール `example_role` を作成します。

```SQL
CREATE ROLE example_role;
```

### ロールの付与

[GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) を使用して、ユーザーまたは他のロールにロールを付与できます。

- ユーザーにロールを付与する。

  次の例は、ユーザー `jack` にロール `example_role` を付与します。

  ```SQL
  GRANT example_role TO USER jack@'172.10.1.10';
  ```

- 他のロールにロールを付与する。

  次の例は、ロール `test_role` にロール `example_role` を付与します。

  ```SQL
  GRANT example_role TO ROLE test_role;
  ```

<GrantToGroup />

### ユーザーのデフォルトロールを変更する

ユーザーのデフォルトロールは、ユーザーが StarRocks に接続すると自動的にアクティブ化されます。接続後にユーザーのすべてのロール（デフォルトおよび付与されたロール）を有効にする方法については、[すべてのロールを有効にする](#enable-all-roles) を参照してください。

[SET DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) または [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) を使用して、ユーザーのデフォルトロールを設定できます。

次の例はどちらも、`jack` のデフォルトロールを `db1_admin` に設定します。`db1_admin` は `jack` に割り当てられている必要があります。

- SET DEFAULT ROLE を使用してデフォルトロールを設定する：

  ```SQL
  SET DEFAULT ROLE 'db1_admin' TO jack@'172.10.1.10';
  ```

- ALTER USER を使用してデフォルトロールを設定する：

  ```SQL
  ALTER USER jack@'172.10.1.10' DEFAULT ROLE 'db1_admin';
  ```

### ロールの取り消し

[REVOKE](../../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して、ユーザーまたは他のロールからロールを取り消すことができます。

> **注意**
>
> システム定義のデフォルトロール `PUBLIC` をユーザーから取り消すことはできません。

- ユーザーからロールを取り消す。

  次の例は、ユーザー `jack` からロール `example_role` を取り消します。

  ```SQL
  REVOKE example_role FROM USER jack@'172.10.1.10';
  ```

- 他のロールからロールを取り消す。

  次の例は、ロール `test_role` からロール `example_role` を取り消します。

  ```SQL
  REVOKE example_role FROM ROLE test_role;
  ```

### ロールの削除

[DROP ROLE](../../../sql-reference/sql-statements/account-management/DROP_ROLE.md) を使用してロールを削除できます。

次の例は、ロール `example_role` を削除します。

```SQL
DROP ROLE example_role;
```

> **注意**
>
> システム定義のロールは削除できません。

### すべてのロールを有効にする

ユーザーのデフォルトロールは、ユーザーが StarRocks クラスターに接続するたびに自動的にアクティブ化されるロールです。

すべての StarRocks ユーザーが StarRocks クラスターに接続するときに、すべてのロール（デフォルトおよび付与されたロール）を有効にしたい場合は、次の操作を行うことができます。

この操作には、システム権限 OPERATE が必要です。

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

また、SET ROLE を使用して割り当てられたロールをアクティブ化することもできます。たとえば、ユーザー `jack@'172.10.1.10'` は `db_admin` と `user_admin` のロールを持っていますが、これらはユーザーのデフォルトロールではなく、ユーザーが StarRocks に接続すると自動的にアクティブ化されません。`jack@'172.10.1.10'` が `db_admin` と `user_admin` をアクティブ化する必要がある場合、`SET ROLE db_admin, user_admin;` を実行できます。SET ROLE は元のロールを上書きします。すべてのロールを有効にしたい場合は、SET ROLE ALL を実行してください。

## 権限の管理

システム定義のロール `user_admin` を持つユーザーは、StarRocks で権限を付与または取り消すことができます。

### 権限の付与

[GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) を使用して、ユーザーまたはロールに権限を付与できます。

- ユーザーに権限を付与する。

  次の例は、テーブル `sr_member` に対する SELECT 権限をユーザー `jack` に付与し、`jack` がこの権限を他のユーザーまたはロールに付与できるようにします（SQL で WITH GRANT OPTION を指定することによって）。

  ```SQL
  GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
  ```

- ロールに権限を付与する。

  次の例は、テーブル `sr_member` に対する SELECT 権限をロール `example_role` に付与します。

  ```SQL
  GRANT SELECT ON TABLE sr_member TO ROLE example_role;
  ```

### 権限の取り消し

[REVOKE](../../../sql-reference/sql-statements/account-management/REVOKE.md) を使用して、ユーザーまたはロールから権限を取り消すことができます。

- ユーザーから権限を取り消す。

  次の例は、テーブル `sr_member` に対する SELECT 権限をユーザー `jack` から取り消し、`jack` がこの権限を他のユーザーまたはロールに付与することを禁止します。

  ```SQL
  REVOKE SELECT ON TABLE sr_member FROM USER jack@'172.10.1.10';
  ```

- ロールから権限を取り消す。

  次の例は、テーブル `sr_member` に対する SELECT 権限をロール `example_role` から取り消します。

  ```SQL
  REVOKE SELECT ON TABLE sr_member FROM ROLE example_role;
  ```

## ベストプラクティス

### マルチサービスアクセス制御

通常、企業所有の StarRocks クラスターは単一のサービスプロバイダーによって管理され、複数の事業部門 (LOB) を維持しています。それぞれの LOB は 1 つ以上のデータベースを使用します。

以下に示すように、StarRocks クラスターのユーザーには、サービスプロバイダーと 2 つの LOB (A と B) のメンバーが含まれます。各 LOB は、アナリストとエグゼクティブの 2 つのロールによって運営されています。アナリストはビジネスレポートを生成および分析し、エグゼクティブはレポートをクエリします。

![User Privileges](../../../_assets/user_privilege_1.png)

LOB A はデータベース `DB_A` を独立して管理し、LOB B はデータベース `DB_B` を管理します。LOB A と LOB B は `DB_C` の異なるテーブルを使用します。`DB_PUBLIC` は両方の LOB のすべてのメンバーがアクセスできます。

![User Privileges](../../../_assets/user_privilege_2.png)

異なるメンバーが異なるデータベースおよびテーブルで異なる操作を行うため、サービスおよびポジションに応じてロールを作成し、各ロールに必要な権限のみを適用し、これらのロールを対応するメンバーに割り当てることをお勧めします。以下に示すように：

![User Privileges](../../../_assets/user_privilege_3.png)

1. システム定義のロール `db_admin`、`user_admin`、および `cluster_admin` をクラスター管理者に割り当て、日常のメンテナンスのために `db_admin` と `user_admin` をデフォルトロールとして設定し、クラスターのノードを操作する必要があるときに手動で `cluster_admin` をアクティブ化します。

   例：

   ```SQL
   GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
   ALTER USER user_platform DEFAULT ROLE db_admin, user_admin;
   ```

2. 各 LOB 内の各メンバーにユーザーを作成し、各ユーザーに複雑なパスワードを設定します。
3. 各 LOB 内の各ポジションに対してロールを作成し、対応する権限を各ロールに適用します。

   各 LOB のディレクターには、彼らの LOB が必要とする最大の権限のコレクションと対応する GRANT 権限をそのロールに付与します（ステートメントで WITH GRANT OPTION を指定することによって）。したがって、彼らはこれらの権限を彼らの LOB のメンバーに割り当てることができます。日常業務で必要な場合は、そのロールをデフォルトロールとして設定します。

   例：

   ```SQL
   GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DB_A TO ROLE linea_admin WITH GRANT OPTION;
   GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON TABLE TABLE_C1, TABLE_C2, TABLE_C3 TO ROLE linea_admin WITH GRANT OPTION;
   GRANT linea_admin TO USER user_linea_admin;
   ALTER USER user_linea_admin DEFAULT ROLE linea_admin;
   ```

   アナリストとエグゼクティブには、対応する権限を持つロールを割り当てます。

   例：

   ```SQL
   GRANT SELECT ON ALL TABLES IN DATABASE DB_A TO ROLE linea_query;
   GRANT SELECT ON TABLE TABLE_C1, TABLE_C2, TABLE_C3 TO ROLE linea_query;
   GRANT linea_query TO USER user_linea_salesa;
   GRANT linea_query TO USER user_linea_salesb;
   ALTER USER user_linea_salesa DEFAULT ROLE linea_query;
   ALTER USER user_linea_salesb DEFAULT ROLE linea_query;
   ```

4. すべてのクラスター ユーザーがアクセスできるデータベース `DB_PUBLIC` については、`DB_PUBLIC` に対する SELECT 権限をシステム定義のロール `public` に付与します。

   例：

   ```SQL
   GRANT SELECT ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public;
   ```

複雑なシナリオでは、他の人にロールを割り当ててロールの継承を実現できます。

たとえば、アナリストが `DB_PUBLIC` のテーブルに書き込みおよびクエリする権限を必要とし、エグゼクティブがこれらのテーブルをクエリすることしかできない場合、`public_analysis` および `public_sales` のロールを作成し、関連する権限をロールに適用し、それらをアナリストおよびエグゼクティブの元のロールに割り当てることができます。

例：

```SQL
CREATE ROLE public_analysis;
CREATE ROLE public_sales;
GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public_analysis;
GRANT SELECT ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public_sales;
GRANT public_analysis TO ROLE linea_analysis;
GRANT public_analysis TO ROLE lineb_analysis;
GRANT public_sales TO ROLE linea_query;
GRANT public_sales TO ROLE lineb_query;
```

### シナリオに基づいてロールをカスタマイズする

<UserPrivilegeCase />