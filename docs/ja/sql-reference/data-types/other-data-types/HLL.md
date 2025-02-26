---
displayed_sidebar: docs
---

# HLL (HyperLogLog)

## 説明

HLL は [approximate count distinct](../../../using_starrocks/distinct_values/Using_HLL.md) に使用されます。

HLL は HyperLogLog アルゴリズムに基づいたプログラムの開発を可能にします。これは、HyperLogLog 計算プロセスの中間結果を保存するために使用されます。HLL はテーブルの値列タイプとしてのみ使用できます。HLL は集計を通じてデータ量を削減し、クエリプロセスを高速化します。推定結果には 1% の誤差が生じる可能性があります。

HLL 列は、インポートされたデータや他の列から生成されます。データがインポートされるとき、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 関数は、どの列が HLL 列を生成するために使用されるかを指定します。HLL は COUNT DISTINCT を置き換え、ロールアップでユニークビュー (UV) を迅速に計算するためによく使用されます。

HLL に使用されるストレージスペースは、ハッシュ値の異なる値によって決まります。ストレージスペースは、次の3つの条件に応じて変わります。

- HLL が空の場合。HLL に値が挿入されず、ストレージコストは最も低く、80 バイトです。
- HLL 内の異なるハッシュ値の数が 160 以下の場合。最高のストレージコストは 1360 バイトです (80 + 160 * 8 = 1360)。
- HLL 内の異なるハッシュ値の数が 160 を超える場合。ストレージコストは 16,464 バイトで固定されています (80 + 16 * 1024 = 16464)。

実際のビジネスシナリオでは、データ量とデータ分布がクエリのメモリ使用量と近似結果の精度に影響を与えます。これらの2つの要因を考慮する必要があります。

- データ量: HLL は近似値を返します。データ量が多いほど、結果はより正確になります。データ量が少ないと、誤差が大きくなります。
- データ分布: 大量のデータと高カーディナリティのディメンション列で GROUP BY を行う場合、データ計算はより多くのメモリを使用します。この状況では HLL は推奨されません。GROUP BY を行わない count distinct や低カーディナリティのディメンション列での GROUP BY を行う場合に推奨されます。
- クエリの粒度: 大きなクエリ粒度でデータをクエリする場合、データ量を削減するために集計テーブルやマテリアライズドビューを使用してデータを事前集計することをお勧めします。

## 関連関数

- [HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md): この関数は、条件を満たすすべてのデータの基数を推定するための集計関数です。分析関数としても使用できます。デフォルトウィンドウのみをサポートし、ウィンドウ句はサポートしていません。

- [HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md): この関数は hll タイプのフィールドを集計し、hll タイプで返す集計関数です。

- HLL_CARDINALITY(hll): この関数は、単一の hll 列の基数を推定するために使用されます。

- [HLL_HASH(column_name)](../../sql-functions/scalar-functions/hll_hash.md): これは HLL 列タイプを生成し、挿入またはインポートに使用されます。インポートの使用方法については、説明を参照してください。

- [HLL_EMPTY](../../sql-functions/scalar-functions/hll_empty.md): これは空の HLL 列を生成し、挿入またはインポート時にデフォルト値を埋めるために使用されます。インポートの使用方法については、説明を参照してください。

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

2. [Stream Load](../../../loading/StreamLoad.md) を使用してデータをロードします。

    ```plain text
    a. テーブル列を使用して HLL 列を生成します。
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
    http://host/api/test_db/test/_stream_load

    b. データ列を使用して HLL 列を生成します。
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
    http://host/api/test_db/test/_stream_load
    ```

3. 次の3つの方法でデータを集計します。（集計せずにベーステーブルを直接クエリすると、approx_count_distinct を使用するのと同じくらい遅くなる可能性があります）

    ```sql
    -- a. HLL 列を集計するためのロールアップを作成します。
    alter table test add rollup test_rollup(dt, set1);

    -- b. UV を計算し、データを挿入するための別のテーブルを作成します。

    create table test_uv(
    dt date,
    id int
    uv_set hll hll_union)
    distributed by hash(id);

    insert into test_uv select dt, id, set1 from test;

    -- c. UV を計算するための別のテーブルを作成します。他の列を hll_hash を通じてテストし、データを挿入し、HLL 列を生成します。

    create table test_uv(
    dt date,
    id int,
    id_set hll hll_union)
    distributed by hash(id);

    insert into test_uv select dt, id, hll_hash(id) from test;
    ```

4. データをクエリします。HLL 列は元の値に直接クエリをサポートしていません。関数を使用してクエリできます。

    ```plain text
    a. 総 UV を計算します。
    select HLL_UNION_AGG(uv_set) from test_uv;

    b. 各日の UV を計算します。
    select dt, HLL_CARDINALITY(uv_set) from test_uv;

    c. テストテーブルの set1 の集計値を計算します。
    select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
    select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
    ```