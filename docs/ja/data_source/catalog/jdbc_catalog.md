---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# JDBC catalog

StarRocks は v3.0 以降で JDBC catalog をサポートしています。

JDBC catalog は、JDBC を通じてアクセスされるデータソースからデータを取り込まずにクエリを実行できる外部 catalog の一種です。

また、JDBC catalog を基に [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を使用して、JDBC データソースからデータを直接変換してロードすることもできます。

JDBC catalog は現在、MySQL と PostgreSQL をサポートしています。

## 前提条件

- StarRocks クラスター内の FEs と BEs または CNs が、`driver_url` パラメータで指定されたダウンロード URL から JDBC ドライバーをダウンロードできること。
- 各 BE または CN ノードの **$BE_HOME/bin/start_be.sh** ファイル内の `JAVA_HOME` が、JRE 環境のパスではなく JDK 環境のパスとして適切に設定されていること。例えば、`export JAVA_HOME = <JDK_absolute_path>` と設定できます。この設定をスクリプトの先頭に追加し、BE または CN を再起動して設定を有効にする必要があります。

## JDBC catalog の作成

### 構文

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメータ

#### `catalog_name`

JDBC catalog の名前。命名規則は以下の通りです：

- 名前には文字、数字 (0-9)、アンダースコア (_) を含めることができます。文字で始まる必要があります。
- 名前は大文字小文字を区別し、長さは 1023 文字を超えてはなりません。

#### `comment`

JDBC catalog の説明。このパラメータはオプションです。

#### `PROPERTIES`

JDBC Catalog のプロパティ。`PROPERTIES` には以下のパラメータを含める必要があります：

| **Parameter**     | **Description**                                                     |
| ----------------- | ------------------------------------------------------------ |
| type              | リソースのタイプ。値を `jdbc` に設定します。           |
| user              | ターゲットデータベースに接続するために使用されるユーザー名。 |
| password          | ターゲットデータベースに接続するために使用されるパスワード。 |
| jdbc_uri          | JDBC ドライバーがターゲットデータベースに接続するために使用する URI。MySQL の場合、URI は `"jdbc:mysql://ip:port"` 形式です。PostgreSQL の場合、URI は `"jdbc:postgresql://ip:port/db_name"` 形式です。詳細は [PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html) を参照してください。 |
| driver_url        | JDBC ドライバー JAR パッケージのダウンロード URL。HTTP URL またはファイル URL がサポートされます。例えば、`https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar` や `file:///home/disk1/postgresql-42.3.3.jar` です。<br />**NOTE**<br />JDBC ドライバーを FE と BE または CN ノードの任意の同じパスに配置し、`driver_url` をそのパスに設定することもできます。この場合、`file:///<path>/to/the/driver` 形式でなければなりません。 |
| driver_class      | JDBC ドライバーのクラス名。一般的なデータベースエンジンの JDBC ドライバークラス名は以下の通りです：<ul><li>MySQL: `com.mysql.jdbc.Driver` (MySQL v5.x およびそれ以前) および `com.mysql.cj.jdbc.Driver` (MySQL v6.x およびそれ以降)</li><li>PostgreSQL: `org.postgresql.Driver`</li></ul> |

> **NOTE**
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
```

## JDBC catalog の表示

現在の StarRocks クラスター内のすべての catalog をクエリするには、[SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) を使用できます。

```SQL
SHOW CATALOGS;
```

また、外部 catalog の作成ステートメントをクエリするには、[SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) を使用できます。以下の例では、`jdbc0` という名前の JDBC catalog の作成ステートメントをクエリします。

```SQL
SHOW CREATE CATALOG jdbc0;
```

## JDBC catalog の削除

JDBC catalog を削除するには、[DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) を使用できます。

以下の例では、`jdbc0` という名前の JDBC catalog を削除します。

```SQL
DROP Catalog jdbc0;
```

## JDBC catalog 内のテーブルをクエリする

1. JDBC 対応クラスター内のデータベースを表示するには、[SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) を使用します。

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

このようなエラーが発生した場合、`jdbc_uri` に渡した URI が無効です。渡した URI を確認し、有効であることを確認してください。詳細については、このトピックの「[PROPERTIES](#properties)」セクションのパラメータ説明を参照してください。