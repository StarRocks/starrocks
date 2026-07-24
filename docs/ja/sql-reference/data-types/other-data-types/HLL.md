---
displayed_sidebar: docs
description: "HLL は近似カウントディスティンクトに使用されます。"
---

# HLL (HyperLogLog)

HLL は[近似カウントディスティンクト](../../../using_starrocks/distinct_values/Using_HLL.md)に使用されます。

HLL は HyperLogLog アルゴリズムに基づくプログラムの開発を可能にします。HyperLogLog の計算プロセスの中間結果を保存するために使用されます。テーブルの値列タイプとしてのみ使用できます。HLL は集計によってデータ量を削減し、クエリプロセスを高速化します。推定結果には 1% の偏差が生じる場合があります。

HLL 列はインポートされたデータまたは他の列のデータに基づいて生成されます。データのインポート時に、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md)関数は HLL 列の生成に使用する列を指定します。HLL は COUNT DISTINCT の代替としてよく使用され、ロールアップを使用してユニークビュー (UV) を素早く計算します。

HLL が使用するストレージ容量は、ハッシュ値の中のディスティンクト値によって決まります。ストレージ容量は以下の 3 つの条件によって異なります：

- HLL が空の場合。HLL に値が挿入されておらず、ストレージコストが最も低く、80 バイトです。
- HLL 内のディスティンクトなハッシュ値の数が 160 以下の場合。最大ストレージコストは 1360 バイトです (80 + 160 * 8 = 1360)。
- HLL 内のディスティンクトなハッシュ値の数が 160 を超える場合。ストレージコストは 16,464 バイトで固定されます (80 + 16 * 1024 = 16464)。

実際のビジネスシナリオでは、データ量とデータ分布がクエリのメモリ使用量と近似結果の精度に影響します。以下の 2 つの要素を考慮する必要があります：

- データ量：HLL は近似値を返します。データ量が多いほど結果は正確になります。データ量が少ないほど偏差が大きくなります。
- データ分布：データ量が多く、GROUP BY の次元列のカーディナリティが高い場合、データ計算はより多くのメモリを使用します。この状況では HLL は推奨されません。グループなしのカウントディスティンクトや、低カーディナリティの次元列に対する GROUP BY を実行する場合に推奨されます。
- クエリ粒度：大きなクエリ粒度でデータをクエリする場合、集計テーブルまたはマテリアライズドビューを使用してデータを事前集計し、データ量を削減することをお勧めします。

## 関連する関数

- [HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md)：この関数は、条件を満たすすべてのデータのカーディナリティを推定するために使用される集計関数です。分析関数としても使用できます。デフォルトウィンドウのみをサポートし、ウィンドウ句はサポートしません。

- [HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md)：この関数は hll 型のフィールドを集計し、hll 型で返す集計関数です。

- HLL_CARDINALITY(hll)：この関数は単一の hll 列のカーディナリティを推定するために使用されます。

- [HLL_HASH(column_name)](../../sql-functions/scalar-functions/hll_hash.md)：HLL 列タイプを生成し、挿入またはインポートに使用されます。インポートの使用方法の説明を参照してください。

- [HLL_EMPTY](../../sql-functions/scalar-functions/hll_empty.md)：空の HLL 列を生成し、挿入またはインポート時にデフォルト値を埋めるために使用されます。インポートの使用方法の説明を参照してください。

## 例

1. HLL 列 `set1` と `set2` を持つテーブルを作成します。

   ```sql
   create table test(
   dt date,
   id int,
   name char(10),
   province char(10),
   os char(1),
   set1 hll hll_union,
   set2 hll hll_union)
   distributed by hash(id);
   ```

2. 以下を使用してデータをロードします[Stream Load](../../../loading/StreamLoad.md)。

   ```plain text
   a. Use table columns to generate an HLL column.
   curl --location-trusted -uname:password -T data -H "label:load_1" \
       -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
   http://host/api/test_db/test/_stream_load

   b. Use data columns to generate an HLL column.
   curl --location-trusted -uname:password -T data -H "label:load_1" \
       -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
   http://host/api/test_db/test/_stream_load
   ```

3. 以下の 3 つの方法でデータを集計します：（集計なしでベーステーブルを直接クエリすると、approx_count_distinct を使用するのと同様に遅くなる場合があります）

   ```sql
   -- a. HLL 列を集計するロールアップを作成します。
   alter table test add rollup test_rollup(dt, set1);

   -- b. UV を計算する別のテーブルを作成し、データを挿入します

   create table test_uv(
   dt date,
   id int,
   uv_set hll hll_union)
   distributed by hash(id);

   insert into test_uv select dt, id, set1 from test;

   -- c. UV を計算する別のテーブルを作成します。データを挿入し、hll_hash を使用して他の列をテストすることで HLL 列を生成します。

   create table test_uv(
   dt date,
   id int,
   id_set hll hll_union)
   distributed by hash(id);

   insert into test_uv select dt, id, hll_hash(id) from test;
   ```

4. データをクエリします。HLL 列は元の値への直接クエリをサポートしていません。マッチング関数を使用してクエリできます。

   ```plain text
   a. Calculate the total UV.
   select HLL_UNION_AGG(uv_set) from test_uv;

   b. Calculate the UV for each day.
   select dt, HLL_CARDINALITY(uv_set) from test_uv;

   c. Calculate the aggregation value of set1 in the test table.
   select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
   select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
   ```
