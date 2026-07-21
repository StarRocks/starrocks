---
displayed_sidebar: docs
sidebar_position: 5
description: "共有データクラスター上でClaudeとStarRocks MCPサーバーを使用して複雑なデータを分析します。"
---

# Claude + StarRocks MCP

小規模なStarRocksクラスターをオブジェクトストレージ上に構築し、実際の公開データセットを読み込み、**Claude**を通じて平易な英語で質問できます**StarRocks MCPサーバー**。Claudeはスキーマを検出し、SQL（複数テーブルの結合を含む）を記述し、チャートを描画します。すべてのデータは**MinIO**のオブジェクトストレージに保存されます。

このチュートリアルの内容：

- StarRocksを共有データモード（FE 1台 + CN 1台）で実行し、MinIOをDockerで起動する
- ストレージとコンピュートを分離するためのS3（MinIO）ストレージボリュームを作成する
- Olistブラジルeコマースデータセット（8つの関連テーブル）を読み込む
- StarRocksおよびAIStor MCPサーバーをClaudeに接続する
- Claudeに平易な英語で質問し、チャートを描画する

データセットは**Olistブラジルeコマース**セット（8つの関連テーブル、複雑な結合に適している）です。すべては**FE 1台 + CN 1台**上で、ノートパソコンまたは無料のクラウド枠で動作します。

このドキュメントには多くの情報が含まれており、ステップバイステップの内容が最初に、参考資料が最後に記載されています。これは、まず環境を構築して質問を始め、その後で補足的な詳細を読めるようにするためです。

***

## 前提条件

### Docker

- [Docker](https://www.docker.com/get-started/)（Docker DesktopまたはエンジンとCompose）
- Dockerに割り当てられた約4GBのRAM

### MySQLクライアント

SQLファイルを実行するためのMySQLクライアント（例：`mysql`）。これはStarRocks FEサーバーによって提供されるため、このガイドのすべてのSQLコマンドは`docker compose exec`で実行します。

### uv

[`uv`](https://docs.astral.sh/uv/)はStarRocks MCPサーバーを実行します。

### Claude CodeまたはClaude Desktop

[Claude Code](https://claude.com/product/claude-code)またはClaude DesktopがMCPサーバーを接続します。

:::tip
このデモにはClaude Codeを使用した手順が含まれています。他のLLMもMCPサーバーで動作しますが、このデモはClaude Codeでのみテストされています。他のLLMでの使用経験についてはIssueを開いてお知らせください。
:::

### Olistデータセット

**Olistデータセット**は`kagglehub`でデータを読み込む際に自動的にダウンロードされます（Kaggleアカウント不要）：[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)。

:::note
Apple Silicon：StarRocksはAVX2（x86）に依存しています。ARMビルドを使用するか、エミュレーションが遅くなることを想定してください。
:::

***

## 用語集

### MCP

モデルコンテキストプロトコル（MCP）は、ClaudeなどのAIアシスタントが外部ツールを検出して呼び出せるようにするオープンプロトコルです。このチュートリアルでは、2つのMCPサーバーを接続します：

- **StarRocks MCPサーバー** (`mcp-server-starrocks`) は、ClaudeがStarRocksのスキーマを読み取り、SQLを実行できるツールを公開します。
- **AIStor MCPサーバー** (`aistor`) は、Claudeがデータの保存先であるMinIOオブジェクトストレージを操作できるツールを公開します。たとえば、バケットの参照やStarRocksが書き込んだオブジェクトの検査などが可能です。

### FE

フロントエンドノードは、メタデータ管理、クライアント接続管理、クエリプランニング、およびクエリスケジューリングを担当します。

### CN

コンピュートノードは、共有データデプロイメントにおけるクエリプランの実行を担当します。

***

## 前提条件のインストール

### `uv`

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows（PowerShell）
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# またはHomebrew / pipx / pip経由
brew install uv
# pipx install uv
# pip install uv
```

他のオプションについては、[公式 `uv` インストールガイド](https://docs.astral.sh/uv/getting-started/installation/) を参照してください。インストール後、`PATH` に追加されていることを確認してください：

```bash
uv --version
```

### Docker

[https://www.docker.com/get-started/](https://www.docker.com/get-started/)

### Claude Code

[https://claude.com/product/claude-code](https://claude.com/product/claude-code)

***

## デモリポジトリのクローン

クローン [https://github.com/StarRocks/demo](https://github.com/StarRocks/demo)：

```bash
gh repo clone StarRocks/demo
```

または

```bash
git clone git@github.com:StarRocks/demo.git
```

:::note
このディレクトリには、作業を開始するための最低限の内容のみが含まれています。意図的に、*いかなる* 定型クエリ、期待される回答、およびウォークスルーは含まれていません。これにより、Claudeが示すすべての内容が、チェックアウト内の資料から想起されたものではなく、ライブスキーマから推論されたものになります。
:::

***

## StarRocksとMinIOの起動

```bash
cd demo/documentation-samples/MCP
docker compose up --detach --wait --wait-timeout 120
```

フロントエンド（FE）、コンピュートノード（CN）、およびMinIOがローカルで起動します。

MinIO、FE、およびCNサービスの正常なステータスを確認します：

```bash
docker compose ps -a --format "table {{.Service}}\t{{.Status}}"
```

:::tip
CNが正常と報告されない場合は、数秒待ってから再度確認してください。CNは最後に起動するサービスです。
:::

***

## MinIOにバケットを作成する

MinIOコンソールを [http://localhost:9001](http://localhost:9001) で開き（ログイン `miniouser` / `M!n10R0cks`）、**バケットの作成** をクリックして、バケット `my-starrocks-bucket` を作成します。

***

## ストレージボリュームの作成

ファイル `storage_volume.sql` は、前のステップで作成したバケットに StarRocks ストレージボリュームを作成し、それをデフォルトとして設定します。ファイルの内容はこのチュートリアルの最後で詳しく説明します。今はこれを実行してください：

```bash
docker compose exec -T starrocks-fe \
  mysql -P9030 -h127.0.0.1 -uroot < storage_volume.sql
```

これは成功する必要があります（かつボリュームがデフォルトである必要があります）。`CREATE TABLE` の前に確認してください。

```sql
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
IsDefault: true
 Location: s3://my-starrocks-bucket/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
```

***

## テーブルを作成する

```bash
docker compose exec -T starrocks-fe \
  mysql -h127.0.0.1 -P9030 -uroot < olist_schema.sql
```

:::tip
`The specified bucket does not exist` というエラーが表示された場合、前のステップの一部をスキップした可能性があります。MinIO UI を開き、上記で指定したバケットを作成してください。
:::

:::tip
これは MCP サーバーを試す良い機会です。現在のディレクトリから Claude Code を起動し、2 つの MCP サーバー（`aistor` と `mcp-server-starrocks`）を許可してから、Claude に `olist` DB のデータベース一覧を表示してスキーマを説明するよう依頼してください。
:::

***

## データを読み込む

Kaggle から Olist の CSV を kagglehub でダウンロードします **kagglehub** — 匿名で利用可能、Kaggle アカウントや API トークンは不要です。ファイルをローカルにキャッシュし、保存先フォルダを表示します。

データをダウンロードし、保存先フォルダを `CSV_DIR` として取得します。kagglehub はバージョン警告を stdout に出力するため、`tail -n1` で最終行（パス）を取得してください：

```bash
export CSV_DIR="$(uv run --with "kagglehub==0.3.12" python \
  -c "import kagglehub; print(kagglehub.dataset_download('olistbr/brazilian-ecommerce'))" \
  | tail -n1)"
echo "$CSV_DIR"   # sanity check: should be a .../brazilian-ecommerce/versions/N path
```

:::note
`kagglehub==0.3.12` のピンは意図的なものです：新しいリリース（1.0.x）は `kagglesdk` ビルドを取得し、インポートに失敗します（`ModuleNotFoundError: kagglesdk.competitions.legacy`）。0.3.12 は公開データセットを匿名でダウンロードでき、正常に動作します。kagglehub はファイルをキャッシュするため、再実行のコストは低いです。
:::

次にローダーを実行します（`CSV_DIR` はすでにエクスポートされています）：

```bash
FE_HTTP_PORT=8040 bash load_olist.sh
```

`FE_HTTP_PORT=8040` は Stream Load を CN に直接送信し、`starrocks-cn` ホスト名のリダイレクト（`/etc/hosts` の編集や sudo 不要）を回避します。スクリプトは最後に行数チェックを出力します。

***

## MCP サーバーを Claude に接続する

```bash
cp .env.example .env
```

`.mcp.json` は 2 つの MCP サーバーを定義しています — `mcp-server-starrocks`（`uv` 経由で GitHub から実行されるため、**手動でインストールするものはありません**）と `aistor`（Docker 経由で実行）— どちらも `.env` を読み込みます。このディレクトリから Claude Code を起動するか、同じブロックを Claude Desktop の設定に追加してください。

:::note
**初回起動時にサーバーを承認してください。** `.mcp.json` からのプロジェクトスコープのサーバーは *ありません* 自動的に信頼されません — Claude Code は *「新しい MCP サーバーが見つかりました — 承認しますか？」* 初回起動時にプロンプトを表示します。承認してください。（事前承認は `.claude/settings.local.json` に保存されており、gitignore されているため、**新規クローン時には存在しません** — これは想定内です。プロンプトで承認すると再作成されます。）

**`/mcp` で接続を確認してください。** Claude Code 内で `/mcp` を実行し、`mcp-server-starrocks` と `aistor` の両方が **接続済み** と表示されていることを確認してください。これが信頼できるステータス確認方法です — サーバーが接続されていない場合、どのように尋ねてもツールは読み込まれません。

**初回起動は時間がかかります。** 最初の `uv run` はサーバーの依存関係（pyarrow、kaleido など、約 115 MB）をダウンロードするため、StarRocks サーバーの初回起動には数分かかる場合があります。2 回目以降の起動は高速です。
:::

`/mcp` で両方のサーバーが接続済みと表示されたら、ツールが利用可能であることを確認してください（例：*「StarRocks MCP サーバーはどのようなツールを提供していますか？」*と尋ねてみてください）。

StarRocks クラスターは `root`（パスワードなし）の `localhost:9030`、データベース `olist` で MySQL プロトコル経由でアクセスできます。

***

## 質問する

データについて平易な英語で Claude に質問してください — まず全体像を把握させるところから始めましょう（例：*「このデータベースにはどのようなテーブルがあり、それらはどのように関連していますか？」*）、そこから探索を広げてください。Claude はライブスキーマを確認し、SQL を自分で記述します。

### おすすめの質問

これらはデモ動画で質問された内容を順番に並べたものです。各質問は前の質問を踏まえて構成されています。まず最初の質問から始めてClaudeに状況を把握させ、順番に進めてください：

- *どんなデータベースとテーブルがありますか？*
- *これらのテーブルはどのように関連していますか？*
- *このデータは実際にどこに保存されていますか？*
- *顧客の州別に注文をプロットしてください。*
- *遠くに配送するほどコストがかかりますか？*
- *重量と距離を分けるために、キロあたりの運賃を示す2本目の線を追加してください。*
- *そのクエリはどのように構築しましたか？*
- *配送の遅延はレビューに悪影響を与えていますか？*
- *定型回答を使っていますか、それともすべての数値はライブテーブルから取得されましたか？*

:::tip
**チャート：** でインタラクティブなチャートをリクエストすると、**HTML** StarRocks MCPサーバーがそれを`DEMO_Output/`に書き込み（`STARROCKS_CHART_OUTPUT_DIR`で`.env`に設定）、静的プレビューがインラインで表示されます。HTMLファイルをブラウザで開くとホバー／ズーム／パンが可能です。
:::

***

## トラブルシューティング

- **StarRocks MCPツールが読み込まれない／サーバーが「接続中のまま」：** プロジェクトサーバーが承認されていませんでした。`/mcp`を実行してください。もし**接続済み**になっていない場合は、このディレクトリからClaude Codeを再起動し、*「新しいMCPサーバーが見つかりました — 承認しますか？」*プロンプトを承認してください（または`"enabledMcpjsonServers": ["mcp-server-starrocks", "aistor"]`を`.claude/settings.local.json`に追加してください）。サーバーを別途インストールする必要は**不要**ありません。`.mcp.json`は`uv`経由でGitHubから実行されます。
- **初回起動には数分かかります：** 最初の`uv run`でサーバーの依存関係（約115 MB）がダウンロードされます。これは初回のみ発生する想定された動作であり、失敗ではありません。
- **`mysql`クライアントが`Authentication plugin 'mysql_native_password' cannot be loaded`で失敗する：** StarRocksの問題ではなく、Homebrewの`mysql`クライアントの特性によるものです。代わりにコンテナ経由でクエリを実行してください：`docker compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW DATABASES;"`。
- **`CREATE TABLE`がハング／タイムアウトする：** CNのAWS SDKがEC2メタデータサービスを探索しています。composeファイルはすでに`starrocks-cn`に`AWS_EC2_METADATA_DISABLED=true`を設定しています。独自のcomposeファイルを使用する場合は追加してください。
- **Stream Loadのリダイレクトエラー：** `FE_HTTP_PORT=8040`を使用して（上記のように）CNに直接POSTするか、`127.0.0.1 starrocks-cn`を`/etc/hosts`に追加してください。
- **`order_reviews`が行を拒否する：** フリーテキストのコメント列に改行が含まれています。`load_olist.sh`に記載されているLEANバリアントを使用してください。

***

## まとめ

このチュートリアルでは以下を実施しました：

- DockerにStarRocksの共有データ構成（FE×1 + CN×1）とMinIOをデプロイした
- S3（MinIO）ストレージボリュームを作成し、デフォルトとして設定した
- Olist Brazilian E-Commerceデータセット（8つの関連テーブル）を読み込んだ
- StarRocksとAIStor MCPサーバーをClaudeに接続しました
- Claudeに平易な英語で質問し、スキーマの探索、SQLの記述、チャートのレンダリングを自動で行わせました

ビルド／セットアップ資料（ナレーション付きウォークスルーおよびビデオスクリプト）は、この環境に定型回答が混入しないよう別途管理されています：

- **StarRocks MCPサーバー：** [https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)

***

## このリポジトリの内容

デモファイルは `documentation-samples/MCP` ディレクトリに格納されています（[StarRocks/demo](https://github.com/StarRocks/demo) リポジトリ内）：

| ファイル | 内容 |
|---|---|
| `CLAUDE.md` | Claudeコードへの指示。作業内容を明示し、外部情報ではなくスキーマに依拠するよう指定しています。このファイルは必ずお読みください。 |
| `.claude/settings.json` | 内容は `"autoMemoryEnabled": false` です。前回セッションで質問した内容（および回答）をClaudeが記憶しないようにするためのものです。不要であれば削除しても構いません。作者の質問が実験に影響しないよう含めています。 |
| `docker-compose.yml` | StarRocksの共有データクイックスタート（FE×1 + CN×1 + MinIO）。ラップトップ／Docker向けにパッチ済み。 |
| `storage_volume.sql` | S3（MinIO）ストレージボリュームを作成し、デフォルトとして設定します。 |
| `olist_schema.sql` | Olistテーブル8件（＋オプション1件）のDDL。 |
| `load_olist.sh` | OlistのCSVをStream Loadで取り込み、行数チェックを実施します。 |
| `.mcp.json` | StarRocksとAIStor MCPサーバーをClaudeに接続します。 |
| `.env.example` | 認証情報／エンドポイントのテンプレート。`cp` を `.env` にコピーし、必要に応じて編集してください。 |

***

## `storage_volume.sql` に関する注意事項

`storage_volume.sql` は2つの処理を行います：タイムアウトを延長し、ストレージボリュームを作成して有効化します。

まず、タブレット作成のタイムアウトを延長し、ストレージボリュームを作成します：

```sql
-- デフォルトは10秒で、オブジェクトストアのタブレット作成には短すぎます
ADMIN SET FRONTEND CONFIG ('tablet_create_timeout_second'='60');

CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES (
        "enabled" = "true",
        "aws.s3.endpoint" = "minio:9000",
        "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
        "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "aws.s3.use_instance_profile" = "false",
        "aws.s3.use_aws_sdk_default_behavior" = "false"
    );
```

次に、新しいストレージボリュームをデフォルトとして設定し、その詳細を表示します：

```sql
SET s3_volume AS DEFAULT STORAGE VOLUME;   -- REQUIRED before CREATE DATABASE/TABLE
DESC STORAGE VOLUME s3_volume\G            -- verify: enabled=true, IsDefault=true
```

***

## 詳細情報

- StarRocks共有データクイックスタート — [shared-data](shared-data.md)
- StarRocks MCPサーバー — [https://github.com/StarRocks/mcp-server-starrocks](https://github.com/StarRocks/mcp-server-starrocks)
- AIStor MCPサーバー — [https://github.com/minio/mcp-server-aistor](https://github.com/minio/mcp-server-aistor)
- Olistデータセット — [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

***

## クレジット＆ライセンス

- **Olist Brazilian E-Commerce** Kaggle経由のデータセット、ライセンス：**CC BY-NC-SA 4.0（非商用）** — 教育目的のデモとして使用。クレジットはOlistに帰属します。商用再利用の前に利用規約をご確認ください。
- StarRocksおよびStarRocks MCPサーバーはオープンソースです（Apache-2.0 ／プロジェクトライセンス）。
- MinIOおよび `mcp-server-aistor` © MinIO, Inc.
