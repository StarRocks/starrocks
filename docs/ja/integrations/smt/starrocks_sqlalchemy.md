---
displayed_sidebar: docs
description: "スキーマ管理と移行"
sidebar_label: "スキーマ管理と移行"
---

# SQLAlchemy と Alembic によるスキーマ管理と移行

このガイドでは、Python エコシステム（SQLAlchemy、Alembic、sqlacodegen を含む）を使用して、**`starrocks` SQLAlchemy** ダイアレクトを通じて StarRocks スキーマを管理する方法を紹介します。**スキーママイグレーションがなぜ有用なのか**、そして **StarRocks で効果的に使用する方法**を理解するのに役立ちます。

## 概要

多くのユーザーは、StarRocks のテーブル、ビュー、マテリアライズドビューを SQL DDL を直接使用して管理しています。しかし、プロジェクトが成長するにつれて、`ALTER TABLE` ステートメントを手動で維持することはエラーを引き起こしやすく、追跡が困難になります。

**StarRocks SQLAlchemy ダイアレクト (`starrocks`)** は以下を提供します：

- StarRocks の **テーブル**、**ビュー**、**マテリアライズドビュー** のための完全な SQLAlchemy モデルレイヤー
- テーブルスキーマとテーブルプロパティ（ビューとマテリアライズドビューを含む）のための **宣言的** 定義
- **Alembic** との統合により、スキーマ変更を自動的に **検出** し、**生成** することが可能
- **sqlacodegen** のようなツールとの互換性により、モデルを逆生成することが可能

これにより、Python ユーザーは StarRocks スキーマを **宣言的**、**バージョン管理された**、**自動化された** 方法で維持することができます。

## 主な利点

スキーママイグレーションは伝統的に OLTP データベースと関連付けられていますが、StarRocks のようなデータウェアハウジングシステムでも価値があります。以下の利点から、チームは [Alembic](https://alembic.sqlalchemy.org/) を StarRocks ダイアレクトと共に使用しています。

### 宣言的スキーマ定義

Python [ORM](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart) モデルや [SQLAlchemy](https://docs.sqlalchemy.org) コアスタイルでスキーマを定義すると、`ALTER TABLE` ステートメントを手動で書く必要がなくなります。

### 自動差分生成と自動生成

Alembic は **現在の StarRocks スキーマ** と **SQLAlchemy モデル** を比較し、マイグレーションスクリプトを自動的に生成します（`CREATE`/`DROP`/`ALTER`）。

### レビュー可能でバージョン管理されたマイグレーション

各スキーマ変更はマイグレーションファイル（Python）となり、ユーザーは変更を追跡し、必要に応じてロールバックできます。

### 環境間での一貫したワークフロー

スキーマ変更は、開発、ステージング、本番環境に同じプロセスで適用できます。

## インストールと接続

### 前提条件

- StarRocks Python クライアント: 1.3.2 以上
- `SQLAlchemy`: 1.4 以上（SQLAlchemy 2.0 が推奨され、`sqlacodegen` を使用するには必須）
- `Alembic`: 1.16 以上

### StarRocks Python クライアントのインストール

以下のコマンドを実行して、StarRocks Python クライアントをインストールします。

```bash
pip install starrocks
```

### StarRocks への接続

以下の URL を使用して、StarRocks クラスターに接続します。

```bash
starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
```

- `user`: クラスターに接続するためのユーザー名。
- `password`: ユーザーパスワード。
- `FE_host`: FE の IP アドレス。
- `query_port`: FE の `query_port`（デフォルト: 9030）。
- `catalog`: データベースが所在するカタログの名前。
- `database`: 接続したいデータベースの名前。

インストール後、以下のコード例を使用して接続性をすぐに検証できます。

```python
from sqlalchemy import create_engine, text

# まず `mydatabase` を作成する必要があります
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

with engine.connect() as conn:
    conn.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
```

## StarRocks モデルの定義（宣言的 ORM）

StarRocks ダイアレクトは以下をサポートします：

- テーブル
- ビュー
- マテリアライズドビュー

また、StarRocks 固有のテーブル属性もサポートしています：

- `ENGINE` (OLAP)
- キーモデル（`DUPLICATE KEY`、`PRIMARY KEY`、`UNIQUE KEY`、`AGGREGATE KEY`）
- `PARTITION BY` のバリエーション（RANGE / LIST / 式に基づくパーティション化）
- `DISTRIBUTED BY` のバリエーション（HASH / RANDOM）
- `ORDER BY`
- テーブルプロパティ（例：`replication_num`、`storage_medium`）

:::important
- StarRocks ダイアレクトオプションは、`starrocks_` というプレフィックスを付けたキーワード引数として渡されます。
- `starrocks_` **プレフィックスは小文字でなければなりません**。サフィックスは大文字小文字どちらでも受け入れられます（例：`PRIMARY_KEY` と `primary_key`）。
- テーブルキーを指定する場合（例：`starrocks_primary_key="id"`）、関与するカラムも `Column(...)` で `primary_key=True` とマークされている必要があります。これにより、SQLAlchemy メタデータと Alembic の自動生成が正しく動作します。
:::

以下の例は、実際の公開 API とパラメータ名を反映しています。

### テーブルの例

StarRocks テーブルオプションは、ORM（`__table_args__` 経由）と Core（`Table(..., starrocks_...=...)` 経由）の両方のスタイルで指定できます。

#### ORM（宣言的）スタイル

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from starrocks import INTEGER, STRING

# クイックテストと同じエンジンを使用
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_orm_table'
    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        'comment': 'table comment',

        'starrocks_primary_key': 'id',
        'starrocks_distributed_by': 'HASH(id) BUCKETS 10',
        'starrocks_properties': {'replication_num': '1'}
    }

# データベースにテーブルを作成
Base.metadata.create_all(engine)
```

#### Core スタイル

```python
from sqlalchemy import Column, MetaData, Table, create_engine
from starrocks import INTEGER, VARCHAR

# クイックテストと同じエンジンを使用
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

metadata = MetaData()

my_core_table = Table(
    'my_core_table',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('name', VARCHAR(50)),

    # StarRocks 固有の引数
    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"}
)

# データベースにテーブルを作成
metadata.create_all(engine)
```

:::note
テーブル属性とデータ型の包括的なリファレンスについては、[Reference [4]](#references) を参照してください。
:::

### ビューの例

以下は、`columns` を辞書のリスト（`name`/`comment`）として使用する推奨されるビュー定義スタイルです。この例は、既存のテーブル `my_core_table` に基づいています。

```python
from starrocks.schema import View

# 上記の Core テーブル例からメタデータを再利用
metadata = my_core_table.metadata

user_view = View(
    "user_view",
    metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)
```

:::note
ビューのオプションと制限については、[Reference [5]](#references) を参照してください。
:::

### マテリアライズドビューの例

マテリアライズドビューは同様に定義されます。`starrocks_refresh` プロパティは、リフレッシュ戦略を示す構文文字列です。

```python
from starrocks.schema import MaterializedView

# 上記の Core テーブル例からメタデータを再利用
metadata = my_core_table.metadata

# シンプルなマテリアライズドビューを作成（非同期リフレッシュ）
user_stats_ = MaterializedView(
    'user_stats_',
    metadata,
    definition='SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id',
    starrocks_refresh='ASYNC'
)
```

:::note
オプションと ALTER の制限については、[Reference [6]](#references) を参照してください。
:::

## Alembic 統合

StarRocks SQLAlchemy ダイアレクトは以下を完全にサポートします：

- テーブルの作成 / 削除
- ビューの作成 / 削除
- マテリアライズドビューの作成 / 削除
- StarRocks 固有の属性に対するサポートされた変更の検出（例：テーブルプロパティと分散）

これにより、Alembic の **autogenerate** が正しく動作します。

### Alembic の初期化

1. Alembic を初期化します：

   ```bash
   alembic init migrations
   ```

2. `alembic.ini` にデータベース URL を設定します：

   ```ini
   # alembic.ini
   sqlalchemy.url = starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
   ```

3. StarRocks ダイアレクトのログを有効にします（オプション）：

   `alembic.ini` で `starrocks` ロガーを有効にして、ログを通じてテーブルの検出された変更を観察できます。詳細については、[Reference [2]](#references) を参照してください。

   `env.py` を編集します（オフラインおよびオンラインパスの両方を設定）：

    ```python
    from alembic import context
    from starrocks.alembic import render_column_type, include_object_for_view_
    from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401  (ensure impl registered)

    from myapp.models import Base  # adjust to your project

    target_metadata = Base.metadata


    def run_migrations_offline() -> None:
        url = context.config.get_main_option("sqlalchemy.url")
        context.configure(
            url=url,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_
        )

        with context.begin_transaction():
            context.run_migrations()


    def run_migrations_online() -> None:
        # ... create engine and connect as in alembic default env.py ...
        with connectable.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                render_item=render_column_type,
                include_object=include_object_for_view_
            )

            with context.begin_transaction():
                context.run_migrations()
    ```

### マイグレーションの自動生成

```bash
alembic revision --autogenerate -m "initial schema"
```

Alembic は SQLAlchemy モデルと実際の StarRocks スキーマを比較し、正しい DDL を出力します。

### マイグレーションの適用

```bash
alembic upgrade head
```

ダウングレードもサポートされています（可能な場合）。

:::important
StarRocks DDL は複数のステートメントにわたってトランザクションをサポートしていません。アップグレードが途中で失敗した場合、すでに適用されたものを確認し、再実行する前に **手動で修正**（例：補償マイグレーションを書くか、手動で DDL を実行）する必要があるかもしれません。
:::

## サポートされているスキーマ変更操作

このダイアレクトは Alembic の自動生成をサポートしています：

- **テーブル**：作成 / 削除、および `starrocks_*` を通じて宣言された StarRocks 固有の属性の差分（StarRocks ALTER サポート内）
- **ビュー**：作成 / 削除 / 変更（主に定義関連の変更；一部の属性は不変）
- **マテリアライズドビュー**：作成 / 削除 / 変更（リフレッシュ戦略やプロパティなどの可変句に限定）

一部の StarRocks DDL 変更は不可逆または変更不可です。これらの変更は、テーブル/ビュー/マテリアライズドビューを削除して再作成することでのみ行うことができます。ダイアレクトでこれらの変更を指定すると、自動生成は **警告またはエラー** を発します。

## エンドツーエンドの例（初心者向け推奨読書）

このセクションでは、生成されたファイルを確認する場所を含む、実行可能なエンドツーエンドのワークフローを示します。

### ステップ 1. プロジェクトディレクトリを作成し、Alembic を初期化

```bash
mkdir my_sr_alembic_project
cd my_sr_alembic_project

alembic init alembic
```

### ステップ 2. `alembic.ini` を設定

`alembic.ini` の URL を編集：

```ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

### ステップ 3. モデルを定義

モデル用のパッケージを作成：

```bash
mkdir -p myapp
touch myapp/__init__.py
```

`myapp/models.py` を作成し、パッケージ内にテーブル/ビュー/マテリアライズドビューの定義を配置：

:::note
Alembic マイグレーションを使用する場合、モデルモジュール内で `metadata.create_all(engine)` を呼び出さないでください。
:::

```python
from sqlalchemy import Column, Table
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from starrocks import INTEGER, STRING, VARCHAR
from starrocks.schema import MaterializedView, View

Base = declarative_base()


# --- ORM テーブル ---
class MyOrmTable(Base):
    __tablename__ = "my_orm_table"

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    name: Mapped[str] = mapped_column(STRING)

    __table_args__ = {
        "comment": "table comment",
        "starrocks_primary_key": "id",
        "starrocks_distributed_by": "HASH(id) BUCKETS 10",
        "starrocks_properties": {"replication_num": "1"},
    }


# --- 同じメタデータ上の Core テーブル（Alembic target_metadata にとって重要） ---
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    comment="core table comment",
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)


# --- ビュー ---
user_view = View(
    "user_view",
    Base.metadata,
    definition="SELECT id, name FROM my_core_table WHERE name IS NOT NULL",
    columns=[
        {"name": "id", "comment": "ID"},
        {"name": "name", "comment": "Name"},
    ],
    comment="Active users",
)


# --- マテリアライズドビュー ---
user_stats_mv = MaterializedView(
    "user_stats_mv",
    Base.metadata,
    definition="SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id",
    starrocks_refresh="ASYNC",
)
```

### ステップ 4. 自動生成のための `env.py` を設定

`alembic/env.py` を編集：

1. `target_metadata` を設定するために `myapp.models` をインポートします。
2. `render_column_type` と `include_object_for_view_mv` をインポートし、`run_migrations_offline()` と `run_migrations_online()` の両方に設定して、ビューと MV を適切に処理し、StarRocks のカラムタイプを適切にレンダリングします。

:::note
`env.py` ファイルを置き換えるのではなく、これらの行を `env.py` に追加または修正する必要があります。
:::

```python
from alembic import context
from starrocks.alembic import render_column_type, include_object_for_view_mv
from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401

from myapp.models import Base

target_metadata = Base.metadata

# オプション：単一の BE 開発クラスター用にバージョンテーブルのレプリケーションを設定
version_table_kwargs = {"starrocks_properties": {"replication_num": "1"}}

# run_migrations_offline() と run_migrations_online() の両方で、以下を確認：
def run_migrations_offline() -> None:
    url = context.config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        render_item=render_column_type,
        include_object=include_object_for_view_mv,
        version_table_kwargs=version_table_kwargs,
    )


def run_migrations_online() -> None:
    # ... alembic デフォルトの env.py のようにエンジンを作成し接続 ...
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_mv,
            version_table_kwargs=version_table_kwargs,
        )
```

### ステップ 5. 最初のリビジョンを自動生成

```bash
alembic revision --autogenerate -m "create initial schema"
```

一時停止してレビュー：

1. `alembic/versions/` の下に生成されたマイグレーションファイルを確認します。
2. 期待される操作（例：`create_table`、`create_view`、`create_materialized_view`）が含まれていることを確認します。
3. 予期しない削除や変更が含まれていないことを確認します。

### ステップ 6. SQL をプレビューして適用

SQL をプレビュー：

```bash
alembic upgrade head --sql
```

一時停止してレビュー：

1. DDL が期待する順序であることを確認します。
2. 潜在的に重い操作を特定し、必要に応じてマイグレーションを分割することを検討します。

適用：

```bash
alembic upgrade head
```

:::important
StarRocks DDL は複数のステートメントにわたってトランザクションをサポートしていません。アップグレードが途中で失敗した場合、すでに適用されたものを確認し、再実行する前に手動で修正する必要があるかもしれません。
:::

### ステップ 7. 変更を加えて再度自動生成

`myapp/models.py` を更新して：

- **既存のテーブル**（`my_core_table`）を変更：カラムを追加、テーブルコメントを更新し、テーブルプロパティを変更。
- **新しいテーブル**（`my_new_table`）を追加。

:::note
カラムを追加することは時間のかかるスキーマ変更です。StarRocks は一度に **1 つのスキーマ変更ジョブのみ** を許可します。実際には、「カラムの追加/削除/変更」の変更を他の重い変更（例：追加のカラムの追加/削除や大量のプロパティ変更）から分離し、必要に応じて複数の Alembic リビジョンに分割することをお勧めします。
:::

```python
from sqlalchemy import Column, Table
from starrocks import INTEGER, VARCHAR

# 既存のテーブルを変更（カラムを追加）
# （既存の my_core_table 定義をその場で更新）
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    Column("age", INTEGER),  # 追加されたカラムのみ

    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"},
)

my_new_table = Table(
    "my_new_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    starrocks_primary_key="id",
    starrocks_distributed_by="HASH(id) BUCKETS 10",
    starrocks_properties={"replication_num": "1"},
)
```

```bash
alembic revision --autogenerate -m "add a new table, change a old table"
```

一時停止してレビュー：

新しいマイグレーションに以下が含まれていることを確認します：
- `my_new_table` の `create_table(...)` と
- `my_core_table` の変更に対する期待される操作（例：カラムの追加 / コメントの設定 / プロパティの設定）。

SQL をプレビューして適用：

```bash
alembic upgrade head --sql
alembic upgrade head
```

## sqlacodegen の使用

[`sqlacodegen`](https://github.com/agronholm/sqlacodegen) は、StarRocks から直接 SQLAlchemy モデルを逆生成できます：

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  --generator tables \
  starrocks://<user>:<password>@<FE_host>:<query_port>/[catalog.]<database> > models.py
```

サポートされているオブジェクト：

- テーブル
- ビュー
- マテリアライズドビュー
- パーティショニング、分散、order-by 句、およびプロパティ

これは、既存の StarRocks スキーマを Alembic にオンボーディングする際に便利です。

上記のコマンドを直接使用して、**エンドツーエンドの例**セクションで定義されたテーブル/ビュー/マテリアライズドビューの Python スクリプトを生成できます。

:::note
- Core スタイルのモデルを生成する際には、`--generator tables` を追加することをお勧めします（ORM ジェネレーターは `NOT NULL` / `NULL` 属性に従ってカラムを再配置する場合があります）。
- キーカラムは `NOT NULL` として生成される場合があります。nullable にしたい場合は、生成されたモデルを手動で調整してください。
:::

## 制限事項とベストプラクティス

- 一部の StarRocks DDL 操作はテーブルを削除して再作成する必要があります。自動生成は、利用できない SQL を静かに生成するのではなく、警告またはエラーを発します。
- キーモデルの変更（例：DUPLICATE KEY から PRIMARY KEY への変更）は `ALTER TABLE` を通じてサポートされていません。明示的な計画を使用してください（通常は削除してバックフィルで再作成）。
- StarRocks は複数のステートメントにわたるトランザクション DDL を提供していません。生成されたマイグレーションをレビューし、運用的に適用してください。マイグレーションが途中で失敗した場合、**手動で** ロールバックを処理する必要があるかもしれません。
- 分散については、`BUCKETS` 句を省略した場合、StarRocks はバケット数を自動的に割り当てる場合があります。このダイアレクトは、その場合にノイズの多い差分を避けるように設計されています。

## まとめ

StarRocks SQLAlchemy ダイアレクトと Alembic 統合を使用すると、次のことが可能です：

- ✔ 宣言的モデルを使用して StarRocks スキーマを定義
- ✔ スキーママイグレーションスクリプトを自動的に検出および生成
- ✔ スキーマの進化をバージョン管理
- ✔ ビューとマテリアライズドビューを宣言的に管理
- ✔ 既存のスキーマを sqlacodegen を使用してリバースエンジニアリング

これにより、StarRocks スキーマ管理が現代の Python データエンジニアリングエコシステムに組み込まれ、環境間のスキーマの一貫性が大幅に簡素化されます。

## 参考文献

[1]: [starrocks-python-client README](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/README.md)

[2]: [Alembic Integration](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/alembic.md)

[3]: [SQLAlchemy details](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md)

[4]: [Table Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/tables.md)

[5]: [View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/views.md)

[6]: [Materialized View Support](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md)
