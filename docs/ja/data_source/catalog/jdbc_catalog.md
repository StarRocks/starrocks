---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# JDBC catalog

StarRocks は v3.0 以降で JDBC catalog をサポートしています。

JDBC catalog は、データを取り込むことなく、JDBC を通じてアクセスされるデータソースからデータをクエリすることを可能にする一種の external catalog です。

また、JDBC catalog を使用して、JDBC データソースからデータを直接変換してロードすることもできます。[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用します。

JDBC catalog は v3.0 以降で MySQL と PostgreSQL をサポートし、v3.2.9 および v3.3.1 以降で Oracle と SQLServer をサポートしています。

## 前提条件

- StarRocks クラスター内の FEs と BEs または CNs が、`driver_url` パラメーターで指定されたダウンロード URL から JDBC ドライバーをダウンロードできること。
- 各 BE または CN ノードの **$BE_HOME/bin/start_be.sh** ファイル内の `JAVA_HOME` が、JRE 環境のパスではなく JDK 環境のパスとして適切に設定されていること。例えば、`export JAVA_HOME = <JDK_absolute_path>` と設定できます。この設定をスクリプトの先頭に追加し、BE または CN を再起動して設定を有効にする必要があります。

## JDBC catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメーター

#### `catalog_name`

JDBC catalog の名前。命名規則は以下の通りです：

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始める必要があります。
- 名前は大文字と小文字を区別し、長さは 1023 文字を超えることはできません。

#### `comment`

JDBC catalog の説明。このパラメーターはオプションです。

#### `PROPERTIES`

JDBC catalog のプロパティ。`PROPERTIES` には以下のパラメーターを含める必要があります：

| **Parameter**     | **Description**                                                     |
| ----------------- | ------------------------------------------------------------ |
| type              | リソースのタイプ。値を `jdbc` に設定します。           |
| user              | ターゲットデータベースに接続するために使用されるユーザー名。 |
| password          | ターゲットデータベースに接続するために使用されるパスワード。 |
| jdbc_uri          | JDBC ドライバーがターゲットデータベースに接続するために使用する URI。MySQL の場合、URI は `"jdbc:mysql://ip:port"` 形式です。PostgreSQL の場合、URI は `"jdbc:postgresql://ip:port/db_name"` 形式です。詳細は [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html) を参照してください。 |
| driver_url        | JDBC ドライバー JAR パッケージのダウンロード URL。HTTP URL またはファイル URL がサポートされます。例えば、`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` や `file:///home/disk1/postgresql-42.3.3.jar` です。<br />**注意**<br />JDBC ドライバーを FE と BE または CN ノードの同じパスに配置し、`driver_url` をそのパスに設定することもできます。この場合、`file:///<path>/to/the/driver` 形式でなければなりません。 |
| driver_class      | JDBC ドライバーのクラス名。一般的なデータベースエンジンの JDBC ドライバークラス名は以下の通りです：<ul><li>MySQL: `com.mysql.jdbc.Driver` (MySQL v5.x およびそれ以前) および `com.mysql.cj.jdbc.Driver` (MySQL v6.x およびそれ以降)</li><li>PostgreSQL: `org.postgresql.Driver`</li></ul> |

> **注意**
>
> FEs は JDBC catalog 作成時に JDBC ドライバー JAR パッケージをダウンロードし、BEs または CNs は最初のクエリ時に JDBC ドライバー JAR パッケージをダウンロードします。ダウンロードにかかる時間はネットワークの状況によって異なります。

### 例

以下の例では、`jdbc0` と `jdbc1` の 2 つの JDBC catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG jdbc0
PROPERTIES
(
    "type"="jdbc", 
    "user"="postgres",
    "password"="changeme",
    "jdbc_uri"="jdbc:postgresql://127.0.0.1:5432/jdbc_test",
    "driver_url"="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class"="org.postgresql.Driver"
);

CREATE EXTERNAL CATALOG jdbc1
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:mysql://127.0.0.1:3306",
    "driver_url"="https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar",
    "driver_class"="com.mysql.cj.jdbc.Driver"
);
 
CREATE EXTERNAL CATALOG jdbc2
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:oracle:thin:@127.0.0.1:1521:ORCL",
    "driver_url"="https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc10/19.18.0.0/ojdbc10-19.18.0.0.jar",
    "driver_class"="oracle.jdbc.driver.OracleDriver"
);
       
CREATE EXTERNAL CATALOG jdbc3
PROPERTIES
(
    "type"="jdbc",
    "user"="root",
    "password"="changeme",
    "jdbc_uri"="jdbc:sqlserver://127.0.0.1:1433;databaseName=MyDatabase;",
    "driver_url"="https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar",
    "driver_class"="com.microsoft.sqlserver.jdbc.SQLServerDriver"
);
       
```

## JDBC catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用します。

```SQL
SHOW CATALOGS;
```

また、external catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用します。以下の例では、`jdbc0` という名前の JDBC catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG jdbc0;
```

## JDBC catalog の削除

JDBC catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用します。

以下の例では、`jdbc0` という名前の JDBC catalog を削除します。

```SQL
DROP Catalog jdbc0;
```

## JDBC catalog のテーブルをクエリする

1. JDBC 互換クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. 現在のセッションで目的の catalog に切り替えるには、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) を使用します。

    ```SQL
    SET CATALOG <catalog_name>;
    ```

    次に、現在のセッションでアクティブなデータベースを指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

    ```SQL
    USE <db_name>;
    ```

    または、目的の catalog 内でアクティブなデータベースを直接指定するには、[USE](../../sql-reference/sql-statements/Database/USE.md) を使用します。

    ```SQL
    USE <catalog_name>.<db_name>;
    ```

3. 指定されたデータベース内の目的のテーブルをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) を使用します。

   ```SQL
   SELECT * FROM <table_name>;
   ```

## FAQ

「Malformed database URL, failed to parse the main URL sections」というエラーが発生した場合はどうすればよいですか？

このようなエラーが発生した場合、`jdbc_uri` に渡した URI が無効です。渡した URI を確認し、有効であることを確認してください。詳細については、このトピックの「[PROPERTIES](#properties)」セクションのパラメーター説明を参照してください。