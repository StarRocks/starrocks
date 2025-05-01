---
displayed_sidebar: docs
---

# HLL

## 説明

HLL は HyperLogLog アルゴリズムに基づくプログラムの開発を可能にします。これは HyperLogLog 計算プロセスの中間結果を保存するために使用されます。テーブルの値カラムタイプとしてのみ使用可能です。HLL は集計を通じてデータ量を削減し、クエリプロセスを高速化します。推定結果には 1% の誤差が生じる可能性があります。

HLL カラムは、インポートされたデータまたは他のカラムから生成されます。データがインポートされる際、[hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) 関数は、どのカラムが HLL カラムを生成するために使用されるかを指定します。HLL は COUNT DISTINCT を置き換えるためによく使用され、ロールアップでユニークビュー (UV) を迅速に計算します。

関連する関数:

[HLL_UNION_AGG(hll)](../../sql-functions/aggregate-functions/hll_union_agg.md): この関数は、条件を満たすすべてのデータの基数を推定するための集計関数です。分析関数としても使用できます。デフォルトウィンドウのみをサポートし、ウィンドウ句はサポートしていません。

[HLL_RAW_AGG(hll)](../../sql-functions/aggregate-functions/hll_raw_agg.md): この関数は、hll タイプのフィールドを集計し、hll タイプで返す集計関数です。

HLL_CARDINALITY(hll): この関数は、単一の hll カラムの基数を推定するために使用されます。

[HLL_HASH(column_name)](../../sql-functions/aggregate-functions/hll_hash.md): これは HLL カラムタイプを生成し、挿入またはインポートに使用されます。インポートの使用方法については、説明を参照してください。

[HLL_EMPTY](../../sql-functions/aggregate-functions/hll_empty.md): これは空の HLL カラムを生成し、挿入またはインポート時にデフォルト値を埋めるために使用されます。インポートの使用方法については、説明を参照してください。

## 例

1. まず、hll カラムを持つテーブルを作成します。

    ```sql
    create table test(
    dt date,
    id int,
    name char(10),
    province char(10),
    os char(1),
    set1 hll hll_union,
    set2 hll hll_union)
    distributed by hash(id) buckets 32;
    ```

2. データをインポートします。インポート方法については [Stream Load](../../../loading/StreamLoad.md) を参照してください。

    ```plain text
    a. テーブルカラムを使用して hll カラムを生成
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
    http://host/api/test_db/test/_stream_load

    b. データカラムを使用して hll カラムを生成
    curl --location-trusted -uname:password -T data -H "label:load_1" \
        -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
    http://host/api/test_db/test/_stream_load
    ```

3. 次の3つの方法でデータを集計します。（集計なしでベーステーブルを直接クエリすると、approx_count_distinct を使用するのと同じくらい遅くなる可能性があります）

    ```plain text
    a. ロールアップを作成して hll カラムを集計
    alter table test add rollup test_rollup(dt, set1);

    b. 別のテーブルを作成して uv を計算し、データを挿入

    create table test_uv(
    dt date,
    id int
    uv_set hll hll_union)
    distributed by hash(id) buckets 32;

    insert into test_uv select dt, id, set1 from test;

    c. 別のテーブルを作成して uv を計算。データを挿入し、hll_hash を通じて他のカラムをテストして hll カラムを生成

    create table test_uv(
    dt date,
    id int,
    id_set hll hll_union)
    distributed by hash(id) buckets 32;

    insert into test_uv select dt, id, hll_hash(id) from test;
    ```

4. クエリ。HLL カラムは元の値への直接クエリをサポートしていません。関数を使用してクエリできます。

    ```plain text
    a. 合計 nv を計算
    select HLL_UNION_AGG(uv_set) from test_uv;

    b. 各日の uv を計算
    select dt, HLL_CARDINALITY(uv_set) from test_uv;

    c. テストテーブルの set1 の集計値を計算
    select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
    select dt, HLL_UNION_AGG(set1) as uv from test group by dt;
    ```