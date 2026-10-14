---
sidebar_position: 160
displayed_sidebar: docs
description: 読み取り専用の外部 Catalog と Catalog 単位のオブジェクトストレージ認証情報を使用して Lance データセットをクエリします。
---

# Lance catalog

Lance Connector は実験的な機能です。FE は Lance Java SDK 12.0.0 を使用してデータセットを検出し、スキーマを読み取ります。BE は Lance Rust SDK と Arrow C Data Interface を通じてデータを読み取ります。FE のメタデータライブラリ、BE Connector、ネイティブ読み取りライブラリを一緒にデプロイしてください。

## Catalog の作成

既存の Lance データセットを含むディレクトリを Warehouse として指定し、Directory Catalog を構成します。Connector は Warehouse 直下のデータセットを検出し、列の null 許容属性を含むスキーマを Lance 形式から読み取ります。例えば、次の Warehouse 内の `vectors.lance` はテーブル `vectors` に対応します。

```sql
CREATE EXTERNAL CATALOG lance_data
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "s3://example-bucket/datasets/",
    "lance.namespace.root_database" = "datasets",
    "aws.s3.access_key" = "<access-key>",
    "aws.s3.secret_key" = "<secret-key>",
    "aws.s3.region" = "us-east-1"
);

SHOW TABLES FROM lance_data.datasets;
SELECT id FROM lance_data.datasets.vectors WHERE id > 10 LIMIT 20;
```

| プロパティ | 説明 |
| --- | --- |
| `lance.catalog.type` | Catalog の実装。デフォルトは `directory` です。`rest` は将来の対応用に予約されており、現在は拒否されます。 |
| `lance.catalog.warehouse` | 必須。Lance データセットを含むローカル絶対パスまたはオブジェクトストレージ URI。 |
| `lance.namespace.root_database` | Warehouse のルートに対応する SQL データベース名。デフォルトは `default` です。サブディレクトリではなくエイリアスです。 |

`table.<name>.uri`、`table.<name>.schema`、従来の `database` プロパティはサポートされません。この Directory 実装では、ネストされた Namespace は公開されません。Connector は既存のデータセットを一覧表示し、Namespace Manifest の作成や更新は行いません。

一時的な S3 認証情報を使用する場合は、`aws.s3.session_token` も設定します。S3 互換エンドポイントには `aws.s3.endpoint`、`aws.s3.enable_path_style_access`、`aws.s3.enable_ssl` を使用します。FE は Catalog のクラウド構成を BE に送信し、読み取り処理で Lance のストレージオプションに変換します。

FE と BE の両方でネイティブ認証情報チェーンを使用するには、`aws.s3.use_aws_sdk_default_behavior = true` を設定します。これは Java AWS SDK ではなく、Lance のネイティブオブジェクトストレージの認証情報解決を使用します。Catalog に明示的に設定する STS ロール引き受け、`aws.s3.use_instance_profile`、`aws.s3.use_web_identity_token_file` はサポートされず、明示的なエラーになります。

## Azure Data Lake Storage Gen2

ストレージアカウントを含むデータセット URI と、データセットファイルの読み取りおよび一覧表示権限を持つ SAS トークンを使用します。

```sql
CREATE EXTERNAL CATALOG lance_azure
PROPERTIES (
    "type" = "lance",
    "lance.catalog.type" = "directory",
    "lance.catalog.warehouse" = "abfss://data@exampleaccount.dfs.core.windows.net/datasets/",
    "lance.namespace.root_database" = "datasets",
    "azure.adls2.storage_account" = "exampleaccount",
    "azure.adls2.sas_token" = "<sas-token>"
);

SELECT id, count(*)
FROM lance_azure.datasets.events
GROUP BY id;
```

Catalog のクラウド構成にある Azure 共有キー、標準の ADLS2 マネージド ID、ADLS2 サービスプリンシパルの認証情報も変換されます。FE と BE の両方のホストからストレージにアクセスできる必要があります。マネージド ID を使用する場合は、両方のホストで利用可能にしてください。サービスプリンシパル認証では、現在 Azure パブリッククラウドのログイン機関が必要です。ワークロード ID のトークンファイル、カスタム Azure ストレージエンドポイント、Catalog の認証情報を使用するアカウント指定のない `az://` URI はサポートされません。サポートされない構成や一致しない構成は、Catalog の認証情報を黙って破棄するのではなくエラーになります。

SAS トークンと一時的な S3 認証情報は静的な Catalog 構成です。Connector はリモートの認証情報サービスから取得したり更新したりしません。新しいクエリを開始する前に、期限が近い認証情報を置き換えてください。SQL 述語やデータセット URI のクエリ文字列に認証情報を含めないでください。

## クエリの動作と制限

- Warehouse に複数の Lance データセットを配置すると、結合、集計、フィルター、射影を使用してクエリできます。スキーマはテーブルの解決時に読み取られるため、Catalog のプロパティで管理する必要はありません。
- 各テーブルスキャンは、データセットのすべての Fragment を含む単一の Scan Range を使用します。Fragment 単位の並列スキャンと Lance への述語プッシュダウンは未実装です。StarRocks がデコード後の行に対して SQL 述語を評価します。
- 列プルーニングでは、射影列と述語に必要な列を読み取ります。`COUNT(*)` は、スカラー列がある場合にそのうちの 1 列を保持します。
- Arrow の Large String と Large Binary がサポートされ、型はデータセットのスキーマから推論されます。符号なし整数はオーバーフローしないように拡張されます。対応は `uint8` → `SMALLINT`、`uint16` → `INT`、`uint32` → `BIGINT`、`uint64` → `DECIMAL(20,0)` です。
- 読み取り処理はスカラー値、日付、タイムスタンプ、リストをサポートします。日付とタイムスタンプは、Unix エポック以前の小数値を含めて UTC セマンティクスを保持します。サポートされないスキーマ変換は行を破棄せず、クエリを失敗させます。
- Catalog は読み取り専用です。ローカルファイル URI は、FE とスキャンを実行する BE の両方で同じパスからアクセスできる必要があります。
- Linux ではネイティブ読み取りライブラリは `be/lib/libstarrocks_lance.so` としてパッケージ化されます。BE のスキャンに Java リーダーや JVM は不要です。FE のメタデータ SDK は、独立した ClassLoader 内で同梱のネイティブライブラリを使用します。

Lance は Rust SDK 12.0.0 を使用し、ビルド時は**デフォルトで無効**です。デフォルトのビルドでは Cargo、Rust ツールチェーンのダウンロード、Lance SDK のパッケージ化をスキップします。FE のメタデータライブラリと、Lance スキャンを実行するすべての BE/CN で明示的に有効にしてください。

```bash
./build.sh --fe --be --with-connector-lance
```

Connector の対象テストを実行するには、次のコマンドを使用します。

```bash
./run-be-ut.sh --with-connector-lance --build-target connector_lance_test --module connector_lance_test --without-java-ext
```

CMake で直接ビルドする場合は `-DWITH_CONNECTOR_LANCE=ON` を使用します。`--without-connector-lance` または `WITH_CONNECTOR_LANCE=OFF` で明示的に無効にできます。明示的なコマンドラインフラグは環境変数より優先されます。デフォルトまたは明示的に無効なビルドでは、以前ビルドした Lance 共有ライブラリも BE パッケージから除外されます。有効な FE パッケージには `fe/lib/lance-metadata-lib` が含まれます。デフォルトの FE パッケージにはこれらの SDK ライブラリが含まれないため、ディレクトリの一覧表示とスキーマ検出には Lance を有効にした FE パッケージが必要です。クエリの実行には、有効にした BE パッケージも必要です。

読み取り処理はデータセットを一度だけ開き、1 回のスキャンのすべてのバッチで同じ不変スナップショットを維持します。FE はまだ、自己結合を含む別々のスキャンオペレーター間で共通のバージョンを固定しません。

ネイティブビルドには Rust 1.96.1 以降が必要です。Linux x86-64 または AArch64 に適切なツールチェーンがない場合、ビルドは固定バージョンの公式ツールチェーンを BE ビルドディレクトリにダウンロードして検証します。Rust の依存関係は `Cargo.lock` で固定されています。`LANCE_BUILD_JOBS` で Rust のコンパイル並列度を制限できます。デフォルトは 2 です。オフラインビルドには、事前にインストールしたツールチェーンと準備済みの Cargo 依存関係キャッシュが必要です。
