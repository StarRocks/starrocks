---
displayed_sidebar: docs
toc_max_heading_level: 4
description: "ADBC catalog を使用して、データを取り込まずに Arrow Flight SQL をクエリします。"
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# ADBC catalog

<Beta />

ADBC catalog は、ネイティブ [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/) ドライバーを通じて外部データソースをクエリするための external catalog です。初期リリースでは Arrow Flight SQL サービスのクエリをサポートします。

FE は ADBC ドライバーを使用してスキーマ、テーブル、Arrow 列型を検出します。BE または CN は同じドライバーを使用してプッシュダウンされた SQL を実行し、返された Arrow ストリームを Arrow C Data インターフェース経由でインポートします。

## 制限事項

- 初期サポート対象は Arrow Flight SQL です。他の ADBC ドライバーの動作は保証されません。
- ADBC catalog は読み取り専用です。
- StarRocks は ADBC の `db_schema` を StarRocks データベースにマッピングします。ADBC の catalog 階層は公開されません。
- 各スキャンは 1 つの ADBC ストリームを使用します。分散読み取りおよびパーティション読み取りはサポートされません。
- 符号付きおよび符号なし整数、浮動小数点数、Decimal、Boolean、UTF-8 文字列、Binary、Date、Timestamp、Null の Arrow 型をサポートします。複雑な Arrow 型はサポートされません。

## 前提条件

### ネイティブ ADBC ドライバーのインストール

StarRocks は、自身でビルドした `libadbc_driver_jni` ブリッジをパッケージに含めます。標準の StarRocks パッケージを使用する場合、このライブラリをダウンロードまたは置き換える必要はありません。

データソースドライバー自体はパッケージに含まれません。StarRocks ADBC Driver Manager と互換性のある ADBC ドライバーを、すべての FE ノードおよびすべての BE または CN ノードにインストールしてください。どちらのプロセスもネイティブドライバーを直接ロードします。

アップストリーム ADBC Driver Manager の検出機構を使用してドライバーを登録します。システムドライバーマニフェストの使用を推奨します。たとえば、Flight SQL ドライバーを論理名 `adbc_driver_flightsql` で登録するには、各ノードに `/etc/adbc/drivers/adbc_driver_flightsql.toml` を作成します。

```toml
manifest_version = 1
name = "ADBC Flight SQL Driver"

[Driver]
entrypoint = "AdbcDriverInit"
shared = "/opt/adbc/lib/libadbc_driver_flightsql.so"
```

StarRocks サービスアカウントがマニフェストと共有ライブラリを読み取れることを確認してください。`ADBC_DRIVER_PATH` に指定したディレクトリなど、別の [ADBC ドライバーマニフェスト検索パス](https://arrow.apache.org/adbc/current/format/driver_manifests.html)も使用できますが、FE と BE または CN の両方のプロセスが同じ検出設定を継承する必要があります。

Catalog プロパティ `driver` には、`.toml` を除いた論理マニフェスト名を指定します。共有ライブラリのパスではありません。StarRocks はパス区切り文字を含む `driver` 値を拒否します。

### FE で Arrow 用に Java NIO を開く

Java 9 以降では、すべての FE ノードの **fe.conf** にある `JAVA_OPTS` に次のオプションを追加し、すべての FE を再起動します。

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

既存の FE オプションを維持して、次のように追加します。

```properties
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED <existing_FE_JAVA_OPTS>"
```

FE は ADBC メタデータの読み取りに Arrow Java を使用するため、このオプションが必要です。BE または CN の C++ ADBC scanner では、この Java オプションは不要です。

標準 FE パッケージは `adbc_jni_library_path` を `${STARROCKS_HOME}/lib/adbc_driver_jni` に設定し、StarRocks がビルドした JNI ブリッジをこのディレクトリにインストールします。パッケージ内のブリッジを移動した場合にのみ、この FE 設定を変更してください。Flight SQL ドライバーや他のデータソースドライバーを指定しないでください。

## ADBC catalog の作成

### 構文

```sql
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### パラメーター

#### `catalog_name`

ADBC catalog の名前。命名規則については、[System limits](../../sql-reference/System_limit.md) を参照してください。

#### `comment`

Catalog のオプションの説明。

#### `PROPERTIES`

| パラメーター | 必須 | 説明 |
|--------------|------|------|
| `type` | はい | `adbc` に設定します。 |
| `driver` | はい | ADBC Driver Manager が解決する論理 ADBC ドライバー名。例：`adbc_driver_flightsql`。ファイルパスは指定しないでください。 |
| `uri` | はい | ドライバーが受け付けるデータソース URI。例：`grpc+tls://flight.example.com:31337`。 |
| `username` | いいえ | ADBC ドライバーに渡すユーザー名。 |
| `password` | いいえ | ADBC ドライバーに渡すパスワード。 |
| `adbc.*` | いいえ | ドライバー固有の ADBC オプション。完全なキーと値が、データベースの初期化前にドライバーへ転送されます。 |

上記以外のトップレベルプロパティは、`adbc.` で始める必要があります。

### Flight SQL の例

次の例では、ネイティブドライバーが `adbc_driver_flightsql` として登録されている Flight SQL エンドポイント用の catalog を作成します。

```sql
CREATE EXTERNAL CATALOG flight_sql
COMMENT "Flight SQL service"
PROPERTIES
(
    "type" = "adbc",
    "driver" = "adbc_driver_flightsql",
    "uri" = "grpc+tls://flight.example.com:31337",
    "username" = "flight_user",
    "password" = "change_me",
    "adbc.flight.sql.rpc.timeout_seconds.connect" = "10"
);
```

Flight SQL サービスで必要な TLS および認証オプションを設定してください。たとえば、Flight SQL ドライバーは `adbc.flight.sql.client_option.*` および `adbc.flight.sql.rpc.*` のオプションを受け付けます。

## ADBC catalog のクエリ

StarRocks データベースとして公開されるスキーマを一覧表示します。

```sql
SHOW DATABASES FROM flight_sql;
```

スキーマ内のテーブルを一覧表示します。

```sql
SHOW TABLES FROM flight_sql.main;
```

完全修飾名を使用してテーブルをクエリします。

```sql
SELECT n_nationkey, n_name
FROM flight_sql.main.nation
WHERE n_nationkey = 24;
```

テーブルをクエリする前に、[SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) と [USE](../../sql-reference/sql-statements/Database/USE.md) を使用することもできます。

StarRocks は、列の射影、サポートされる述語、`LIMIT` を、ADBC ドライバー経由で実行される SQL にプッシュダウンします。

## ADBC catalog の表示と削除

すべての catalog、または特定の catalog の作成文を表示します。

```sql
SHOW CATALOGS;
SHOW CREATE CATALOG flight_sql;
```

Catalog を削除します。

```sql
DROP CATALOG flight_sql;
```

StarRocks catalog を削除しても、外部 ADBC ドライバーは削除されず、リモートデータソースも変更されません。

## トラブルシューティング

### ドライバーをロードできない

エラーに `Could not load` または `dlopen() failed` が含まれる場合、すべての FE および BE または CN ノードで次を確認してください。

- マニフェストのファイル名が `<driver>.toml` と一致している。
- マニフェストがデフォルトの ADBC 検索ディレクトリ、または `ADBC_DRIVER_PATH` に指定されたディレクトリにある。
- マニフェスト内の共有ライブラリパスが存在し、StarRocks サービスアカウントから読み取り可能である。
- ライブラリがノードの OS および CPU アーキテクチャと一致し、すべてのネイティブ依存関係をロードできる。

### StarRocks JNI ブリッジが見つからない

StarRocks がビルドした JNI ブリッジが見つからないと FE が報告した場合、標準パッケージに `lib/adbc_driver_jni/libadbc_driver_jni.so` が含まれ、`adbc_jni_library_path` がそのディレクトリを指していることを確認してください。この設定は JNI ブリッジ用であり、外部 ADBC データソースドライバー用ではありません。

### Arrow が Java NIO にアクセスできない

Catalog メタデータへのアクセス時に Java モジュールアクセスまたは `java.nio` リフレクションエラーが発生した場合、すべての FE の `JAVA_OPTS` に `--add-opens=java.base/java.nio=ALL-UNNAMED` が含まれ、FE が再起動されていることを確認してください。
