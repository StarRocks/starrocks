---
displayed_sidebar: docs
description: "StarRocksでArrow Flight SQLを使用して最速の大規模結果セット読み取りを実現するための、ベストプラクティスとクライアント側のコードパターン。"
keywords: ['arrow flight sql', 'performance', 'best practices', 'optimization', 'parquet', 'jdbc']
---

# Arrow Flight SQL ベストプラクティス

Arrow Flight SQL は、StarRocks から大規模な結果セットを取得する最速の方法です。同じハードウェア・同じクラスターに対して MySQL プロトコルと比較した場合、Arrow Flight は一貫して高速です：**3×～9×**生プロトコルフェッチで高速、かつ**19×～97×**pandas DataFrame へのエンドツーエンドで高速です。正確な倍率は行数、列の形状、および比較対象の MySQL クライアントによって異なります。ただし、高速化は自動的に得られるわけではありません。クライアントコードが結果を読み取る方法がエンドツーエンドの時間に大きく影響し、いくつかの単純なミスによってその大部分が失われる可能性があります。

このページでは、期待できる全体的な数値を示し、それらに影響する要素をまとめ、各要素についてコードの変更と測定された影響を説明します。

## 全体的なパフォーマンス

以下に2つの比較を示します。1つ目は**プロトコルフェッチ**のみを測定します — 言語レベルのオブジェクト変換なしで、バイトが到着して解析されるまでの時間です。2つ目は、データが `pandas` DataFrame に読み込まれる実際の Python アプリケーションを測定します。ハードウェアについては[テスト環境](#test-environment)を参照してください。

### プロトコルレベルのフェッチ（Arrow Flight ADBC vs `mysql --quick`）

`fetch_arrow_table()` はセルを Python オブジェクトに変換せずにネットワークを Arrow バッファに排出します。`mysql --quick` は行を解析するストリーミング C クライアントで MySQL ワイヤープロトコルを排出します。どちらもプロトコルのみであり、言語ネイティブのオブジェクトのマテリアライズのコストは発生しません。

| ワークロード | 行数 | MySQL プロトコル<br />(`mysql --quick`) | Arrow Flight<br />(`fetch_arrow_table`) | 高速化倍率 |
| --- | --- | --- | --- | --- |
| 単一数値列 (`SELECT id`) | 1 M | 831 ms | 215 ms | **3.9×** |
| 単一数値列 (`SELECT id`) | 5 M | 2,216 ms | 456 ms | **4.9×** |
| 単一数値列 (`SELECT id`) | 10 M | 4,166 ms | 1,163 ms | **3.6×** |
| 単一数値列 (`SELECT id`) | 100 M | 35,629 ms | 6,737 ms | **5.3×** |
| 20 数値列 (`SELECT *`) | 1 M | 1,994 ms | 370 ms | **5.4×** |
| 20 数値列 (`SELECT *`) | 5 M | 9,665 ms | 1,251 ms | **7.7×** |
| 20 数値列 (`SELECT *`) | 10 M | 18,461 ms | 2,577 ms | **7.2×** |
| 20 数値列 (`SELECT *`) | 100 M | 178,416 ms | 19,047 ms | **9.4×** |
| 20 VARCHAR 列 (`SELECT *`) | 1 M | 4,549 ms | 1,294 ms | **3.5×** |
| 20 VARCHAR 列 (`SELECT *`) | 5 M | 19,077 ms | 5,959 ms | **3.2×** |
| 20 VARCHAR 列 (`SELECT *`) | 10 M | 36,079 ms | 11,499 ms | **3.1×** |
| 20 VARCHAR 列 (`SELECT *`) | 100 M | 370,858 ms | 164,508 ms（チャンク） | **2.3×** |

### 実世界のPythonアプリケーション — `pd.read_sql`（ADBC対PyMySQL）

Pythonの標準的なパイプラインは`pd.read_sql(sql, conn) → pandas.DataFrame`です。渡す接続オブジェクトが移行の全てを決定します。PyMySQLの`Connection`を渡すと、pandasは`cursor.fetchall()`と`pd.DataFrame(rows)`を呼び出し、すべての行を走査してDataFrameを構築します。ADBC Flight SQL接続を渡すと、pandasはADBCのネイティブArrowフェッチとほぼゼロコピーのDataFrame変換を使用します。

| ワークロード | 行数 | `pd.read_sql(sql,`<br />`adbc_conn)` | `pd.read_sql(sql,`<br />`pymysql_conn)` | 高速化倍率 |
| --- | --- | --- | --- | --- |
| 単一数値列（`SELECT id`） | 1 M | 320 ms | 6,185 ms | **19.3×** |
| 単一数値列（`SELECT id`） | 5 M | 421 ms | 30,751 ms | **73.0×** |
| 単一数値列（`SELECT id`） | 10 M | 970 ms | 61,524 ms | **63.4×** |
| 単一数値列（`SELECT id`） | 100 M | 6,024 ms | 585,556 ms | **97.2×** |
| 20数値列（`SELECT *`） | 1 M | 522 ms | 27,521 ms | **52.7×** |
| 20数値列（`SELECT *`） | 5 M | 1,530 ms | 141,500 ms | **92.5×** |
| 20数値列（`SELECT *`） | 10 M | 2,689 ms | 255,408 ms | **95.0×** |
| 20数値列（`SELECT *`） | 100 M | 24,568 ms | OOM | — |
| 20 VARCHAR列（`SELECT *`） | 1 M | 1,560 ms | 31,407 ms | **20.1×** |
| 20 VARCHAR列（`SELECT *`） | 5 M | 6,937 ms | 154,560 ms | **22.3×** |
| 20 VARCHAR列（`SELECT *`） | 10 M | 13,260 ms | 304,647 ms | **23.0×** |

各セルは*フェッチ* + *変換* = *合計*；高速化倍率は合計対合計です。狭い数値クエリが最大の比率を示すのは、PyMySQL側がフェッチ中にセルごとにPythonの`int`を割り当て、pandasが変換時にタプルリストを走査するためです — ADBC側はその両方のコストを省略します。Arrowの列指向メモリ形式は二重に有利です：フェッチ中のセルごとのPythonオブジェクト割り当てを省略し、その後のDataFrame変換をほぼ無コストにします。

既存のコードがすでに`pd.read_sql`を使用している場合、移行は1行で済みます：

```python
import adbc_driver_manager
import adbc_driver_flightsql.dbapi as fl
import pandas as pd

with fl.connect(
        uri="grpcs://host:443",
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: "admin",
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: "...",
        }) as conn:
    df = pd.read_sql("SELECT * FROM my_table LIMIT 5000000", conn)
```

## テスト環境

| コンポーネント | 詳細                                                                                                                                      |
| --- |----------------------------------------------------------------------------------------------------------------------------------------------|
| クライアントホスト | AWS EC2 `t3.2xlarge`、クラスターと同じVPCサブネット                                                                                         |
| クラスター | 3 FE + 2 BE（`m6g.xlarge`）；Arrow Flightは`grpcs://…:443`、MySQLは`:9030`                                                               |
| Javaスタック | OpenJDK 17、`jdbc:arrow-flight-sql`、`arrow-jdbc`、`parquet-hadoop`                                                                          |
| Pythonスタック | `python` 3.12、`pyarrow` 24.0、`adbc-driver-flightsql` 1.11、`PyMySQL` 1.2                                                                   |
| ワークロード | 20列のテーブル2つ（VARCHAR中心のものと全整数のもの）および単一列プロジェクション；`SELECT … LIMIT N`による1 M、5 M、10 M、100 Mの行数 |
| MySQLドレインモード | 全測定において`cursor.fetchall()`バッファリング                                                                                            |

## クライアントの選択

コードレベルのチューニングを行う前に、最も重要な単一の決定は、どのクライアントAPIを通じて結果を読み取るかです。[Arrow Flight SQL を使用して StarRocks と対話する](./arrow_flight.md) Python ADBC、Arrow Flight JDBCドライバー、Java ADBCドライバー、およびネイティブ`FlightClient`の完全なセットアップについては、こちらをご覧ください。パフォーマンスの観点では、これらは2つのパスに集約されます：

- **`FlightSqlClient` または ADBC（推奨）を使用した生の Arrow バッチ。** これは、Flight SQL プロトコルが設計された列指向のエンドツーエンドパスです。コードは `VectorSchemaRoot` バッチを受け取り、プリミティブを返すベクターアクセサーで読み取ります。行ごとのオブジェクト割り当ては発生しません。エンドツーエンド（ドレイン＋型変換）では、このパスは約**10×** 1000万行の数値データにおいて Java MySQL JDBC より高速であり、**最大 97×** 1億行の狭い数値クエリを pandas DataFrame として取得する場合に PyMySQL より高速です。ダウンストリームのコードが列指向データ（Pandas、Arrow、ML パイプライン、Parquet ライター、カスタム分析）を利用できる場合は常にこのパスを使用してください。
- **Arrow Flight JDBC ドライバー（`jdbc:arrow-flight-sql`）。** 既存の JDBC コードパスへのドロップイン `ResultSet` が必要な場合、または Tableau、Power BI、DBeaver などの JDBC インターフェイスが必要な BI ツールで使用する場合に利用してください。JDBC の API はドライバーにすべてのセルに対してボックス化された `Object` を返すことを強制するため、このパスは生の Arrow バッチのパフォーマンスには達しません。JDBC ドライバーは MySQL JDBC よりも大幅に高速であり、JDBC 互換性が要件である場合に適切なツールです。

以降の各側面のテーブルではベースラインが切り替わります。Java Arrow Flight JDBC ドライバーと Java MySQL JDBC を比較しており、PyMySQL との比較ではありません。Java MySQL JDBC コネクターは PyMySQL よりも行のマテリアライズが大幅に高速です。たとえば、同じ 500万行の VARCHAR `SELECT *` は Java MySQL JDBC では約 22 秒かかるのに対し、PyMySQL では約 105 秒かかります。そのため、Java の比率は全体的なパフォーマンスの Python の数値より小さくなります。Java ドライバーを選択する際は、Java MySQL JDBC が適切なベースラインです。

以下の 4 つの側面は、選択したクライアントの中で適用されます。側面 1 は JDBC コンシューマー向け、側面 2〜3 は生バッチコンシューマー向け、側面 4 はどちらからの Parquet 出力にも対応します。

## パフォーマンスに影響する要因

上記の高速化は、クライアントコードが Arrow 向けに記述されていることを前提としています。以下の 4 つの側面はそれぞれ、適切なワークロードにおいて 2 倍以上の差をもたらします。これらを正しく設定することが、上記テーブルの「チューニング済み」列と MySQL と変わらないフェッチとの違いです。

1. **JDBC アクセサーメソッド。** 数値列には型付きキャストとともに `rs.getObject(i)` を使用してください。`rs.getString(i)` はドライバーにすべての値を文字列としてフォーマットさせます。
2. **ベクター解決のスコープ。** 生の `VectorSchemaRoot` バッチを消費する際は、行ループの外でバッチごとに 1 回 `FieldVector` を解決してください。行ごとに解決するのではありません。
3. **数値向けの型付き `.get(i)`。** 数値ベクターでは、型付き `.get(i)` は割り当てなしでプリミティブ値を直接返します。汎用アクセサーはすべての値をボックス化します。
4. **Parquet ライターの選択。** PyArrow は行ごとのコードなしに Arrow ストリームから直接 Parquet を書き込みます。Java にはこれに相当する既製ライブラリがなく、すべての Java パスでは `parquet-hadoop` の上に手書きの `WriteSupport<VectorSchemaRoot>` が必要です。

### 側面 1 — JDBC: 型付き列アクセスを使用する

Arrow Flight JDBC ドライバーを使用する場合は、`rs.getObject(i)` を使用して期待される Java 型にキャストしてください。これにより、ドライバーは追加の変換ステップなしにネイティブ Java 型を直接返すことができ、数値列で最も効果があります。

```java
while (rs.next()) {
    Integer id   = (Integer) rs.getObject(1);
    String  name = (String)  rs.getObject(2);
    Long    ts   = (Long)    rs.getObject(3);
}
```

### ベンチマーク: JDBC アクセサーメソッド（ネットワーク含む）

| ワークロード | MySQL JDBC | Arrow Flight JDBC、型付き `getObject()` | 高速化 |
| --- | --- | --- | --- |
| VARCHAR — 500万行 | 22,651 ms | 12,660 ms | **1.79×** |
| VARCHAR — 1000万行 | 49,216 ms | 27,646 ms | **1.78×** |
| 数値 — 500万行 | 16,043 ms | 3,123 ms | **5.14×** |
| 数値 — 1000万行 | 38,134 ms | 9,123 ms | **4.18×** |

### 側面 2 — 生の Arrow バッチ: ベクターを事前解決して型付きアクセスを使用する

ネイティブ `FlightSqlClient` を介して生の Arrow バッチを消費する場合（つまり `VectorSchemaRoot` オブジェクトを反復処理する場合）、2 つのルールに従ってください。

**ベクターをバッチごとに 1 回解決し、行ごとには解決しない。** 行ループの前に `root.getVector("column_name")` を呼び出して、ルックアップコストを行ごとではなくバッチごとに 1 回支払うようにしてください。

**数値ベクターには型付き `.get(i)` を使用する。** これはプリミティブ値を直接返します — ヒープ割り当てなし、GC プレッシャーなし。

```java
IntVector      idVec    = (IntVector)      root.getVector("id");
SmallIntVector yearVec  = (SmallIntVector) root.getVector("birth_year");
TinyIntVector  monthVec = (TinyIntVector)  root.getVector("birth_month");

for (int i = 0; i < rowCount; i++) {
    record.id         = idVec.get(i);     // int   — no allocation
    record.birthYear  = yearVec.get(i);   // short — no allocation
    record.birthMonth = monthVec.get(i);  // byte  — no allocation
}
```

### ベンチマーク: Arrow バッチ変換コスト（事前フェッチ済み）

以下のArrow Flightの数値は変換コストを単独で示しています。バッチはまずクラスターからメモリにドレインされ、その後タイミングが計測されます。

| ワークロード | MySQL JDBC | 型付き `.get*()`、ベクターはバッチごとに1回解決 | 高速化率 |
| --- | --- | --- | --- |
| VARCHAR — 500万行 | 22,651 ms | 11,921 ms | **1.90×** |
| VARCHAR — 1,000万行 | 49,216 ms | 24,686 ms | **1.99×** |
| 数値 — 500万行 | 16,043 ms | 1,141 ms | **14.1×** |
| 数値 — 1,000万行 | 38,134 ms | 2,092 ms | **18.2×** |

### 側面3 — 結果をParquetに書き込む

Apache Arrowには`VectorSchemaRoot`用のビルド済みParquetライターが含まれていません。クエリ結果をParquetファイルにエクスポートすることが目的であれば、[INSERT INTO FILES](./unload_using_insert_into_files.md) を使用すると、StarRocksがクライアント側の変換コードなしにサーバー側でファイルを書き込めます。以下のオプションは、Arrow Flight経由でクライアント側のParquet出力が必要な場合に適用されます。

### オプション1: PyArrowを使用したPython（推奨）

PyArrowはカスタムの書き込みロジックなしにArrow → Parquet変換を処理します。列の型（INT32、INT64、TIMESTAMP_MICROSなど）をネイティブに保持します。

**Arrow Flightからバッチごとにストリーミングする場合:**

```python
import pyarrow.flight as fl
import pyarrow.parquet as pq

client = fl.connect("grpc+tls://host:443")
info   = client.get_flight_info(
    fl.FlightDescriptor.for_command(b"SELECT ..."))

reader = client.do_get(info.endpoints[0].ticket)
with pq.ParquetWriter("output.parquet", reader.schema_arrow, compression="snappy") as writer:
    for batch in reader:
        writer.write_batch(batch)
```

**結果全体がメモリに収まる場合:**

```python
table = reader.read_all()
pq.write_table(table, "output.parquet", compression="snappy")
```

**ADBC（推奨されるPython Flight SQLクライアント）経由:**

```python
import adbc_driver_flightsql.dbapi as fl_sql
import pyarrow.parquet as pq

with fl_sql.connect("grpcs://host:443", db_kwargs={"username": "admin", "password": "..."}) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table LIMIT 5000000")
        pq.write_table(cur.fetch_arrow_table(), "output.parquet", compression="snappy")
```

### オプション2: Java WriteSupport

Javaの場合、`org.apache.parquet:parquet-hadoop`の上にカスタム`WriteSupport<VectorSchemaRoot>`を構築します。ジョブごとにスキーマとライターを一度構築し、`WriteSupport.write()`内で型付きベクター読み取りを使用します。

**スキーマとライターを一度構築する:**

```java
MessageType parquetSchema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
ParquetWriter<VectorSchemaRoot> writer = /* build once per job */;

// バッチごと:
writer.write(batch);
```

**`WriteSupport.write()`内で型付き読み取りを使用する:**

```java
class ArrowWriteSupport extends WriteSupport<VectorSchemaRoot> {
    private RecordConsumer recordConsumer;

    @Override
    public void prepareForWrite(RecordConsumer consumer) {
        this.recordConsumer = consumer;
    }

    @Override
    public void write(VectorSchemaRoot root) {
        int rowCount = root.getRowCount();
        for (FieldVector vec : root.getFieldVectors()) {
            if (vec instanceof IntVector) {
                IntVector iv = (IntVector) vec;
                for (int row = 0; row < rowCount; row++) {
                    recordConsumer.addInteger(iv.get(row));
                }
            } // else if (vec instanceof SmallIntVector) ... BigIntVector ... VarCharVector ...
        }
    }
}
```

### Parquetベンチマーク

数値にはParquetエンコードとファイルI/Oコストの両方が含まれています（[テスト環境](#test-environment)を参照）。VARCHARテーブルと数値テーブルはArrowエンコードパスの異なる部分に負荷をかけるため、別々にベンチマークされています。VARCHAR列は可変長データのオフセットバッファ演算を必要とし、数値列は固定幅の型付きベクターを使用するため、型付きアクセスによる利得がはるかに大きくなります。

#### Java（500万行および1,000万行）

どちらの行も同じ`parquet-hadoop`書き込みパス（`MySqlParquetConverter` + `arrow-jdbc`アダプター、バッチサイズ65,536）を使用しているため、唯一の変数はインバウンドJDBCドライバーです。

| アプローチ | 行数 | VARCHAR 非圧縮 | MySQL比 | VARCHAR Snappy | MySQL比 | 数値 非圧縮 | MySQL比 | 数値 Snappy | MySQL比 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet | 500万 | 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 500万 | **46,341 ms** | **1.20×** | **47,881 ms** | **1.15×** | **13,978 ms** | **1.72×** | **14,297 ms** | **1.77×** |
| MySQL JDBC → Parquet | 1,000万 | 110,229 ms | 1.0× | 116,999 ms | 1.0× | 50,509 ms | 1.0× | 49,126 ms | 1.0× |
| Arrow Flight JDBC → Parquet | 1,000万 | **91,386 ms** | **1.21×** | **102,534 ms** | **1.14×** | **29,739 ms** | **1.70×** | **30,102 ms** | **1.63×** |

#### Python (PyArrow 24.0.0 / ADBC 1.11.0)

MySQLのベースラインは上記の表にあるJava MySQL JDBC → Parquetの数値と同じです。「MySQL → PyArrow」は、`arrow-jdbc`以外にMySQL → Arrowアダプターが存在しないため、実際のパスではありません。Pythonの数値は5Mのみで収集されました。

| アプローチ | VARCHAR 非圧縮 | MySQL比 | VARCHAR Snappy | MySQL比 | 数値 非圧縮 | MySQL比 | 数値 Snappy | MySQL比 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL JDBC → Parquet（Javaベースライン、5M） | 55,477 ms | 1.0× | 54,888 ms | 1.0× | 24,006 ms | 1.0× | 25,289 ms | 1.0× |
| Arrow Flight + PyArrow（5M） | **10,675 ms** | **5.20×** | **14,128 ms** | **3.89×** | **3,953 ms** | **6.07×** | **3,848 ms** | **6.57×** |

PyArrowは生のネットワークフェッチに対してほとんどオーバーヘッドを追加せず、Javaパスよりもはるかに少ないコードで済みます。JavaがハードRequirementでない限り、PyArrowを使用してください。

### まとめ

| ユースケース | 推奨事項 |
| --- | --- |
| Arrow Flight JDBC | 型付きキャストで `getObject()` を使用 |
| 生の `VectorSchemaRoot` バッチ | バッチごとに一度ベクターを解決し、数値列には型付き `.get(i)` を使用 |
| Arrow → Python での Parquet | ADBC経由の `pyarrow.parquet` — 単一の関数呼び出し、カスタムコード不要 |
| Arrow → Java での Parquet | 型付きベクター読み取りによる手書きの `WriteSupport<VectorSchemaRoot>` |
