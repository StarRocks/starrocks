---
displayed_sidebar: docs
---

# Prepared statements

バージョン v3.2 以降、StarRocks は、同じ構造で異なる変数を持つ SQL ステートメントを複数回実行するための prepared statements を提供しています。この機能は、実行効率を大幅に向上させ、SQL インジェクションを防ぎます。

## 説明

prepared statements は基本的に以下のように動作します。

1. **準備**: ユーザーは変数をプレースホルダー `?` で表した SQL ステートメントを準備します。FE は SQL ステートメントを解析し、実行計画を生成します。
2. **実行**: 変数を宣言した後、ユーザーはこれらの変数をステートメントに渡して実行します。ユーザーは異なる変数で同じステートメントを複数回実行できます。

**利点**

- **解析のオーバーヘッドを削減**: 実際のビジネスシナリオでは、アプリケーションは同じ構造で異なる変数を持つステートメントを複数回実行することがよくあります。prepared statements がサポートされている場合、StarRocks は準備段階でステートメントを一度だけ解析する必要があります。異なる変数での同じステートメントの後続の実行は、事前に生成された解析結果を直接使用できます。このようにして、特に複雑なクエリにおいて、ステートメントの実行パフォーマンスが大幅に向上します。
- **SQL インジェクション攻撃を防止**: ステートメントを変数から分離し、ユーザー入力データを直接ステートメントに変数として連結するのではなく、パラメータとして渡すことで、StarRocks は悪意のあるユーザーが悪意のある SQL コードを実行するのを防ぐことができます。

**使用法**

prepared statements は現在のセッションでのみ有効であり、他のセッションでは使用できません。現在のセッションが終了すると、そのセッションで作成された prepared statements は自動的に削除されます。

## 構文

prepared statement の実行は以下のフェーズで構成されます。

- PREPARE: 変数をプレースホルダー `?` で表したステートメントを準備します。
- SET: ステートメント内で変数を宣言します。
- EXECUTE: 宣言された変数をステートメントに渡して実行します。
- DROP PREPARE または DEALLOCATE PREPARE: prepared statement を削除します。

### PREPARE

**構文:**

```SQL
PREPARE <stmt_name> FROM <preparable_stmt>
```

**パラメータ:**

- `stmt_name`: prepared statement に与えられる名前で、後にその prepared statement を実行または解放するために使用されます。この名前は単一のセッション内で一意でなければなりません。
- `preparable_stmt`: 変数のプレースホルダーが疑問符 (`?`) である SQL ステートメント。現在、**`SELECT` ステートメントのみ**がサポートされています。

**例:**

特定の値をプレースホルダー `?` で表した `SELECT` ステートメントを準備します。

```SQL
PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';
```

### SET

**構文:**

```SQL
SET @var_name = expr [, ...];
```

**パラメータ:**

- `var_name`: ユーザー定義の変数の名前。
- `expr`: ユーザー定義の変数。

**例:** 変数を宣言します。

```SQL
SET @id1 = 1, @id2 = 2;
```

詳細については、[ユーザー定義変数](../user_defined_variables.md) を参照してください。

### EXECUTE

**構文:**

```SQL
EXECUTE <stmt_name> [USING @var_name [, @var_name] ...]
```

**パラメータ:**

- `var_name`: `SET` ステートメントで宣言された変数の名前。
- `stmt_name`: prepared statement の名前。

**例:**

変数を `SELECT` ステートメントに渡してそのステートメントを実行します。

```SQL
EXECUTE select_by_id_stmt USING @id1;
```

### DROP PREPARE または DEALLOCATE PREPARE

**構文:**

```SQL
{DEALLOCATE | DROP} PREPARE <stmt_name>
```

**パラメータ:**

- `stmt_name`: prepared statement の名前。

**例:**

prepared statement を削除します。

```SQL
DROP PREPARE select_by_id_stmt;
```

## 例

### prepared statements を使用する

以下の例は、StarRocks テーブルからデータを挿入、削除、更新、およびクエリするために prepared statements を使用する方法を示しています。

`demo` という名前のデータベースと `users` という名前のテーブルが既に作成されていると仮定します。

```SQL
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE users (
  id BIGINT NOT NULL,
  country STRING,
  city STRING,
  revenue BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

1. 実行のためのステートメントを準備します。

    ```SQL
    PREPARE select_all_stmt FROM 'SELECT * FROM users';
    PREPARE select_by_id_stmt FROM 'SELECT * FROM users WHERE id = ?';
    ```

2. これらのステートメントで変数を宣言します。

    ```SQL
    SET @id1 = 1, @id2 = 2;
    ```

3. 宣言された変数を使用してステートメントを実行します。

    ```SQL
    -- テーブルからすべてのデータをクエリします。
    EXECUTE select_all_stmt;

    -- ID 1 または 2 のデータを個別にクエリします。
    EXECUTE select_by_id_stmt USING @id1;
    EXECUTE select_by_id_stmt USING @id2;
    ```

### Java アプリケーションでの Prepared Statements の使用

以下の例は、Java アプリケーションが JDBC ドライバーを使用して StarRocks テーブルからデータを挿入、削除、更新、およびクエリする方法を示しています。

1. JDBC で StarRocks の接続 URL を指定する際、サーバーサイドの prepared statements を有効にする必要があります。

    ```Plaintext
    jdbc:mysql://<fe_ip>:<fe_query_port>/useServerPrepStmts=true
    ```

2. StarRocks の GitHub プロジェクトには、JDBC ドライバーを介して StarRocks テーブルからデータを挿入、削除、更新、およびクエリする方法を説明する [Java コード例](https://github.com/StarRocks/starrocks/blob/main/fe/fe-core/src/test/java/com/starrocks/analysis/PreparedStmtTest.java) が提供されています。