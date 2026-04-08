---
displayed_sidebar: docs
---

# StarRocks JDBC ドライバー

StarRocksは、JDBC互換のクライアント、IDE、またはアプリケーションから直接接続を可能にするネイティブJDBCドライバーを提供します。

## 前提条件

- Java 8以降
- 稼働中のStarRocksクラスター

## ダウンロード

StarRocks JDBCドライバーは以下で入手可能です。[Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j)。

JARをMaven Centralから直接ダウンロードするか、以下の手順に従ってプロジェクトの依存関係として追加できます。

## プロジェクトでJARを使用する

### Maven

以下の依存関係を`pom.xml`に追加します。

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-connector-j</artifactId>
    <version>1.1.1</version>
</dependency>
```

### Gradle

以下の依存関係を`build.gradle`に追加します。

```groovy
implementation 'com.starrocks:starrocks-connector-j:1.1.1'
```

### プレーンJava

JARを以下からダウンロードし、[Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j)コンパイル時および実行時にクラスパスに追加します。

```bash
javac -cp starrocks-connector-j-<version>.jar MyApp.java
java -cp .:starrocks-connector-j-<version>.jar MyApp
```

## 接続URL形式

```
jdbc:starrocks://<fe_host>:<fe_query_port>/<catalog>.<database>
```

| パラメータ | 説明 |
|-----------|-------------|
| `fe_host` | StarRocksクラスターのFEホストIPアドレス。 |
| `fe_query_port` | FEクエリポート、デフォルトは`9030`。 |
| `catalog` | 接続するカタログ。内部テーブルには`default_catalog`を使用するか、外部カタログの名前を使用します。 |
| `database` | カタログ内のデータベース。 |

**例:**

```
jdbc:starrocks://192.168.1.1:9030/default_catalog.my_database
```

## 接続プロパティ

| プロパティ | 説明 |
|----------|-------------|
| `user` | StarRocksにログインするためのユーザー名。例: `admin`。 |
| `password` | StarRocksにログインするためのパスワード。 |

## メタデータ検出

StarRocks JDBCドライバーは、標準のJDBCメタデータAPI (`DatabaseMetaData`) をサポートしており、これによりツールはカタログ、スキーマ、テーブル、および列をイントロスペクトできます。これにより、スキーマブラウジング、オートコンプリート、テーブルイントロスペクションなどのIDE機能がすぐに利用可能になります。

## 例: Javaからの接続

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class StarRocksExample {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:starrocks://192.168.1.1:9030/default_catalog.my_database";
        Connection conn = DriverManager.getConnection(url, "admin", "password");

        try (Statement stmt = conn.createStatement(); 
             ResultSet rs = stmt.executeQuery("SELECT * FROM my_table LIMIT 10")) {
             while (rs.next()) {
                 System.out.println(rs.getString(1));
             }
        }

        conn.close();
    }
}
```
