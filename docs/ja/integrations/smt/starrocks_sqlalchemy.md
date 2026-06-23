---
displayed_sidebar: docs
description: "スキーマ管理と移行"
sidebar_label: "スキーマ管理と移行"
---

# SQLAlchemyとAlembicを使用したスキーマ管理とマイグレーション

このガイドでは、SQLAlchemy、Alembic、sqlacodegen などのPythonエコシステムを使用して、**`starrocks` SQLAlchemy** ダイアレクトを通じてStarRocksスキーマを管理する方法を紹介します。このガイドは、**スキーママイグレーションが有用な理由** と **StarRocksで効果的に使用する方法** を理解するのに役立つように設計されています。

## 概要

多くのユーザーは、SQL DDLを直接使用してStarRocksのテーブル、ビュー、マテリアライズドビューを管理しています。しかし、プロジェクトが成長するにつれて、`ALTER TABLE` ステートメントを手動で管理することはエラーが発生しやすく、追跡が困難になります。

**StarRocks SQLAlchemyダイアレクト（`starrocks`）** は以下を提供します：

- StarRocksの **テーブル**、**ビュー**、および **マテリアライズドビュー**
- **宣言的** のテーブルスキーマとテーブルプロパティ（ビューおよびマテリアライズドビューを含む）の定義
- **Alembic** との統合により、スキーマ変更を自動的に **検出** し、マイグレーションスクリプトを **生成** できます
- **sqlacodegen** などのツールとの互換性により、モデルのリバース生成が可能です

これにより、PythonユーザーはStarRocksスキーマを **宣言的**、**バージョン管理された**、および **自動化された** な方法で管理できます。

## 主なメリット

スキーママイグレーションは従来OLTPデータベースと関連付けられていますが、StarRocksのようなデータウェアハウジングシステムでも価値があります。チームが [Alembic](https://alembic.sqlalchemy.org/) を StarRocks ダイアレクトと併用する理由として、以下のようなメリットがあります。

### 宣言的スキーマ定義

Pythonでスキーマを定義すると、[ORM](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#orm-quickstart)モデルまたは[SQLAlchemy](https://docs.sqlalchemy.org)コアスタイルを使用することで、`ALTER TABLE`ステートメントを手動で記述する必要がなくなります。

### 自動差分検出とマイグレーションスクリプトの自動生成

Alembicは**現在のStarRocksスキーマ**と**SQLAlchemyモデル**を比較し、マイグレーションスクリプトを自動的に生成します（`CREATE`/`DROP`/`ALTER`）。

### レビュー可能なバージョン管理されたマイグレーション

スキーマの変更はそれぞれマイグレーションファイル（Python）として記録されるため、変更履歴の追跡や必要に応じたロールバックが可能です。

### 環境をまたいだ一貫したワークフロー

スキーマの変更は、開発環境・ステージング環境・本番環境に対して同じプロセスで適用できます。

## インストールと接続

### 前提条件

- StarRocks Pythonクライアント: 1.3.2以降
- `SQLAlchemy`: 1.4以降（SQLAlchemy 2.0を推奨。`sqlacodegen`を使用するには必須）
- `Alembic`: 1.16以降

### StarRocks Pythonクライアントのインストール

以下のコマンドを実行して、StarRocks Pythonクライアントをインストールします。

```bash
pip install starrocks
```

### StarRocksへの接続

以下のURLを使用して、StarRocksクラスターに接続します。

```bash
starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
```

- `user`: クラスターへの接続に使用するユーザー名。
- `password`: ユーザーパスワード。
- `FE_host`: FE IPアドレス。
- `query_port`: FEの`query_port`（デフォルト: 9030）。
- `catalog`: データベースが存在するカタログの名前。
- `database`: 接続するデータベースの名前。

インストール後、以下のコード例を使用して接続を簡単に確認できます。

```python
from sqlalchemy import create_engine, text

# 最初に`mydatabase`を作成する必要があります
engine = create_engine("starrocks://root@localhost:9030/mydatabase")

with engine.connect() as conn:
    conn.execute(text("SELECT 1")).fetchall()
    print("Connection successful!")
```

## StarRocksモデルの定義（宣言的ORM）

StarRocksダイアレクトは以下をサポートしています:

- テーブル
- ビュー
- マテリアライズドビュー

また、以下のようなStarRocks固有のテーブル属性もサポートしています:

- `ENGINE` (OLAP)
- キーモデル (`DUPLICATE KEY`, `PRIMARY KEY`, `UNIQUE KEY`, `AGGREGATE KEY`)
- `PARTITION BY` のバリアント (RANGE / LIST / Expression パーティショニング)
- `DISTRIBUTED BY` のバリアント (HASH / RANDOM)
- `ORDER BY`
- テーブルプロパティ (例: `replication_num`, `storage_medium`)

:::important

- StarRocks ダイアレクトのオプションは、`starrocks_` をプレフィックスとするキーワード引数として渡されます。
- `starrocks_` **プレフィックスは小文字でなければなりません**。サフィックスは大文字・小文字どちらでも受け付けられます (例: `PRIMARY_KEY` および `primary_key`)。
- テーブルキー (例: `starrocks_primary_key="id"`) を指定する場合、関連するカラムは**必ず** `Column(...)` 内で `primary_key=True` としてもマークされる必要があります。これにより、SQLAlchemy のメタデータおよび Alembic の自動生成が正しく動作します。
:::

以下の例は、実際の公開 API とパラメータ名を反映しています。

### テーブルの例

StarRocks のテーブルオプションは、ORM スタイル (`__table_args__` 経由) および Core スタイル (`Table(..., starrocks_...=...)` 経由) の両方で指定できます。

#### ORM (宣言型) スタイル

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

# データベースにテーブルを作成する
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

    # StarRocks固有の引数
    starrocks_primary_key='id',
    starrocks_distributed_by='HASH(id) BUCKETS 10',
    starrocks_properties={"replication_num": "1"}
)

# データベースにテーブルを作成する
metadata.create_all(engine)
```

:::note
テーブル属性とデータ型の包括的なリファレンスについては、[リファレンス [4]](#references).
:::

### ビューの例

以下は推奨されるビュー定義スタイルで、`columns` をディクショナリのリスト (`name`/`comment`) として使用します。この例は既存のテーブル `my_core_table` に基づいています。

```python
from starrocks.schema import View

# 上記のCoreテーブルの例からメタデータを再利用する
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
その他のビューオプションと制限事項については、[リファレンス [5]](#references).
:::

### マテリアライズドビューの例

マテリアライズドビューも同様に定義されます。`starrocks_refresh` プロパティは、リフレッシュ戦略を示す構文文字列です。

```python
from starrocks.schema import MaterializedView

# 上記のCoreテーブルの例からメタデータを再利用する
metadata = my_core_table.metadata

# シンプルなマテリアライズドビューを作成する（非同期リフレッシュ）
user_stats_ = MaterializedView(
    'user_stats_',
    metadata,
    definition='SELECT id, COUNT(*) AS cnt FROM my_core_table GROUP BY id',
    starrocks_refresh='ASYNC'
)
```

:::note
その他のオプションおよび ALTER の制限事項については、[リファレンス [6]](#references).
:::

## Alembic との統合

StarRocks SQLAlchemy ダイアレクトは以下を完全にサポートしています:

- テーブルの作成 / 削除
- ビューの作成 / 削除
- マテリアライズドビューの作成 / 削除
- StarRocks 固有の属性 (例: テーブルプロパティや分散) に対するサポートされている変更の検出

これにより、Alembic の**autogenerate** が正しく動作するようになります。

### Alembicの初期化

1. Alembicを初期化する:

   ```bash
   alembic init migrations
   ```

2. `alembic.ini`でデータベースURLを設定する:

   ```ini
   # alembic.ini
   sqlalchemy.url = starrocks://<user>:<password>@<FE_host>:<query_port>/[<catalog>.]<database>
   ```

3. StarRocksダイアレクトのログを有効にする（オプション）:

   `alembic.ini`で`starrocks`ロガーを有効にすると、ログを通じてテーブルの検出された変更を確認できます。詳細については、[参考文献 [2]](#references)。

   `env.py`を編集する（オフラインとオンラインの両方のパスを設定）:

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
       # ... alembicのデフォルトenv.pyと同様にエンジンを作成して接続する ...
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

AlembicはSQLAlchemyモデルと実際のStarRocksスキーマを比較し、正しいDDLを出力します。

### マイグレーションの適用

```bash
alembic upgrade head
```

ダウングレードもサポートされています（可逆的な場合）。

:::important
StarRocksのDDLは複数のステートメントにわたってトランザクション処理されません。アップグレードが途中で失敗した場合、再実行する前に、すでに適用された内容を確認し、**手動による修復を実施する**（例えば、補償マイグレーションを記述するか、手動でDDLを実行する）必要があります。
:::

## サポートされているスキーマ変更操作

このダイアレクトはAlembicの自動生成をサポートしています:

- **テーブル**: 作成 / 削除、および`starrocks_*`で宣言されたStarRocks固有の属性の差分（StarRocks ALTERサポートの範囲内）
- **ビュー**: 作成 / 削除 / 変更（主に定義に関連する変更。一部の属性は変更不可）
- **マテリアライズドビュー**: 作成 / 削除 / 変更（リフレッシュ戦略やプロパティなどの変更可能な句に限定）

一部のStarRocksのDDL変更は可逆的でないか、変更できません。これらの変更を行うには、テーブル/ビュー/マテリアライズドビューを削除して再作成するしかありません。ダイアレクトでこれらの変更を指定した場合、自動生成は**警告または例外を発生させます**。

## エンドツーエンドの例（初心者向け推奨読書）

このセクションでは、生成されたファイルを一時停止して確認する箇所を含む、実行可能なエンドツーエンドのワークフローを示します。

### ステップ1. プロジェクトディレクトリを作成してAlembicを初期化する

```bash
mkdir my_sr_alembic_project
cd my_sr_alembic_project

alembic init alembic
```

### ステップ2. `alembic.ini`を設定する

`alembic.ini`のURLを編集する:

```ini
sqlalchemy.url = starrocks://root@localhost:9030/mydatabase
```

### ステップ3. モデルを定義する

モデル用のパッケージを作成する:

```bash
mkdir -p myapp
touch myapp/__init__.py
```

`myapp/models.py`を作成し、テーブル/ビュー/マテリアライズドビューの定義をパッケージに記述する:

:::note
Alembicマイグレーションを使用する場合、モデルモジュール内で`metadata.create_all(engine)`を呼び出さないでください。
:::

```python
from sqlalchemy import Column, Table
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

from starrocks import INTEGER, STRING, VARCHAR
from starrocks.schema import MaterializedView, View

Base = declarative_base()


# --- ORMテーブル ---
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


# --- 同じメタデータ上のCoreテーブル（Alembicのtarget_metadataにとって重要）---
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

### ステップ4. 自動生成のために`env.py`を設定する

`alembic/env.py`を編集する:

1. `target_metadata`を設定するために`myapp.models`をインポートする。
2. ビューとMVを適切に処理し、StarRocksのカラム型を適切にレンダリングするために、`render_column_type`と`include_object_for_view_mv`をインポートし、`run_migrations_offline()`と`run_migrations_online()`の両方に設定する。

:::note
生成された`env.py`ファイルを置き換えるのではなく、`env.py`にこれらの行を追加または変更する必要があります。
:::

```python
from alembic import context
from starrocks.alembic import render_column_type, include_object_for_view_mv
from starrocks.alembic.starrocks import StarRocksImpl  # noqa: F401

from myapp.models import Base

target_metadata = Base.metadata

# オプション：シングルBE開発クラスター用にバージョンテーブルのレプリケーションを設定する
version_table_kwargs = {"starrocks_properties": {"replication_num": "1"}}

# run_migrations_offline()とrun_migrations_online()の両方で、以下を確認する：
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
    # ... alembicのデフォルトenv.pyと同様にエンジンを作成して接続する ...
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_item=render_column_type,
            include_object=include_object_for_view_mv,
            version_table_kwargs=version_table_kwargs,
        )
```

### ステップ5. 最初のリビジョンを自動生成する

```bash
alembic revision --autogenerate -m "create initial schema"
```

一時停止して確認:

1. `alembic/versions/` の下に生成されたマイグレーションファイルを確認してください。
2. 期待される操作（例: `create_table`、`create_view`、`create_materialized_view`）が含まれていることを確認してください。
3. 予期しないドロップや変更が含まれていないことを確認してください。

### ステップ6. SQLをプレビューして適用する

SQLのプレビュー:

```bash
alembic upgrade head --sql
```

一時停止して確認:

1. DDLが期待する順序になっていることを確認してください。
2. 負荷の高い操作を特定し、必要に応じてマイグレーションの分割を検討してください。

適用:

```bash
alembic upgrade head
```

:::important
StarRocks の DDL は複数のステートメントにわたってトランザクション処理されません。アップグレードが途中で失敗した場合、再実行前にすでに適用済みの内容を確認し、手動で修正を行う必要がある場合があります。
:::

### ステップ7. 変更を加えて再度自動生成する

`myapp/models.py` を以下のように更新してください:

- **既存のテーブルを変更する** (`my_core_table`): カラムを追加するか、テーブルコメントを更新し、テーブルプロパティを1つ変更してください。
- **新しいテーブルを追加する** (`my_new_table`)。

:::note
カラムの追加は時間のかかるスキーマ変更になる場合があります。StarRocks では、**テーブルごとに同時に実行できるスキーマ変更ジョブは1つ** に制限されています。実際には、カラムの追加/削除/変更の変更は、他の重い変更（例: 追加のカラム追加/削除やプロパティの一括変更）とは分けて管理し、必要に応じて複数の Alembic リビジョンに分割することを推奨します。
:::

```python
from sqlalchemy import Column, Table
from starrocks import INTEGER, VARCHAR

# 既存のテーブルを変更する（カラムを追加する）
# （既存のmy_core_tableの定義をその場で更新する。）
my_core_table = Table(
    "my_core_table",
    Base.metadata,
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(50)),
    Column("age", INTEGER),  # added column only

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

一時停止して確認:

新しいマイグレーションに以下が含まれていることを確認してください:

- `my_new_table` に対する `create_table(...)`、および
- `my_core_table` の変更に対する期待される操作（例: カラムの追加 / コメントの設定 / プロパティの設定）。

SQLをプレビューして適用:

```bash
alembic upgrade head --sql
alembic upgrade head
```

## sqlacodegen の使用

[`sqlacodegen`](https://github.com/agronholm/sqlacodegen) は StarRocks から SQLAlchemy モデルを直接リバース生成できます:

```bash
sqlacodegen --options include_dialect_options,keep_dialect_types \
  --generator tables \
  starrocks://<user>:<password>@<FE_host>:<query_port>/[catalog.]<database> > models.py
```

サポートされるオブジェクト:

- テーブル
- ビュー
- マテリアライズドビュー
- パーティショニング、ディストリビューション、ORDER BY 句、およびプロパティ

これは既存の StarRocks スキーマを Alembic に取り込む際に便利です。

上記のコマンドを直接使用して、**エンドツーエンドの例** セクションで定義されたテーブル/ビュー/マテリアライズドビューの Python スクリプトを生成できます。

:::note

- Core スタイルのモデルを生成する際は `--generator tables` を追加することを推奨します（ORM ジェネレーターは `NOT NULL` / `NULL` 属性に従ってカラムを並び替える場合があります）。
- キーカラムは `NOT NULL` として生成される場合があります。NULL 許容にしたい場合は、生成されたモデルを手動で調整してください。
:::

## 制限とベストプラクティス

- StarRocksのDDL操作の一部では、テーブルの削除と再作成が必要です。autogenerateは、利用できないSQLを黙って生成するのではなく、警告またはエラーを発生させます。
- キーモデルの変更（例：DUPLICATE KEYからPRIMARY KEYへの変更）は`ALTER TABLE`ではサポートされていません。明示的なプラン（通常はバックフィルを伴うテーブルの削除と再作成）を使用してください。
- StarRocksは複数のステートメントにわたるトランザクションDDLを提供していません。生成されたマイグレーションを確認し、運用上適用してください。マイグレーションが途中で失敗した場合、ロールバックを手動で処理する必要があります**手動で**。
- ディストリビューションについて、`BUCKETS`句を省略した場合、StarRocksはバケット数を自動割り当てする場合があります。その場合、ダイアレクトはノイズの多い差分を避けるよう設計されています。
- **ビューおよびマテリアライズドビューの定義比較（StarRocks < 4.0.6）：**4.0.6より古いクラスターでは、StarRocksはビュー定義を保存時に正規形式に書き換えるため、モデルに記述したSQLはクラスターが返すものと構文的に異なる場合があります。ダイアレクトはこれを解決するために、比較前にDBの正規形式を取得するための一時ビューを通じてモデルSQLをラウンドトリップさせます。これには、モデル定義で参照されているすべてのテーブルとビューがデータベースにすでに存在している必要があります。存在しない場合（例：それらのオブジェクトも作成するフォワードマイグレーション中）、ダイアレクトは正規表現ベースのノーマライザーにフォールバックします。これは最も一般的な書き換えパターンをカバーしていますが、すべてのエッジケースを処理できるわけではありません。
- **クラスターアップグレード後のビュー定義のドリフト：**StarRocksクラスターがビューが既に存在する状態でアップグレードされた場合、それらのビューは旧バージョンによって正規化されており、アップグレードされたクラスターが生成する逐語的な形式と一致しない場合があります。autogenerateはその期間中に不要なビューマイグレーションを出力する可能性があります。影響を受けるビューを再作成（マイグレーションを削除して再適用）することでドリフトが解消されます。

## まとめ

StarRocks SQLAlchemyダイアレクトとAlembicインテグレーションを使用すると、以下が可能です：

- ✔ 宣言的モデルを使用してStarRocksスキーマを定義する
- ✔ スキーママイグレーションスクリプトを自動検出・生成する
- ✔ スキーマの進化にバージョン管理を使用する
- ✔ ビューおよびマテリアライズドビューを宣言的に管理する
- ✔ sqlacodegen を使用して既存のスキーマをリバースエンジニアリングする

これにより、StarRocksのスキーマ管理が現代のPythonデータエンジニアリングエコシステムに統合され、クロス環境でのスキーマ一貫性が大幅に簡素化されます。

## 参考文献

[1]: [starrocks-python-client README](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/README.md)

[2]: [Alembicインテグレーション](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/alembic.md)

[3]: [SQLAlchemyの詳細](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/sqlalchemy.md)

[4]: [テーブルサポート](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/tables.md)

[5]: [ビューサポート](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/views.md)

[6]: [マテリアライズドビューサポート](https://github.com/StarRocks/starrocks/blob/main/contrib/starrocks-python-client/docs/usage_guide/materialized_views.md)
