---
displayed_sidebar: docs
sidebar_position: 110
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# Flat JSON

<Beta />

この記事では、Flat JSON の基本概念とこの機能の使用方法を紹介します。

バージョン 2.2.0 から、StarRocks は JSON データ型をサポートし、より柔軟なデータストレージを可能にしています。しかし、JSON をクエリする際、ほとんどのシナリオでは JSON データ全体を直接読み取るのではなく、指定されたパスのデータにアクセスします。例えば：

```SQL
-- 必要なフィールドをログに固定フィールドとして保存し、頻繁に変更される他のフィールドを JSON としてパッケージ化します。
SELECT
    time,
    event,
    user,
    get_json_string(remain_json, "$.from_system"),
    get_json_string(remain_json, "$.tag")
FROM logs;
```

JSON 型の特性により、クエリのパフォーマンスは標準型（INT、STRING など）ほど良くありません。その理由は以下の通りです：
- ストレージのオーバーヘッド：JSON は半構造化型であり、各行の構造情報を保存する必要があるため、ストレージの使用量が多く、圧縮効率が低いです。
- クエリの複雑さ：クエリはランタイムデータに基づいてデータ構造を検出する必要があり、ベクトル化された実行最適化を達成するのが難しいです。
- 冗長データ：クエリは JSON データ全体を読み取る必要があり、多くの冗長なフィールドを含んでいます。

StarRocks は、JSON データのクエリ効率を向上させ、JSON の使用の複雑さを軽減するために Flat JSON 機能を導入しました。
- この機能はバージョン 3.3.0 から利用可能で、デフォルトでは無効になっており、手動で有効にする必要があります。

## Flat JSON とは

Flat JSON の核心原理は、ロード中に JSON データを検出し、JSON データから共通フィールドを抽出して標準型データとして保存することです。JSON をクエリする際、これらの共通フィールドが JSON のクエリ速度を最適化します。データの例：

```Plaintext
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

上記の JSON データをロードする際、フィールド `a` と `b` はほとんどの JSON データに存在し、類似のデータ型（どちらも INT）を持っています。したがって、フィールド `a` と `b` のデータは JSON から抽出され、2 つの INT 列として別々に保存されます。これらの 2 つの列がクエリで使用されるとき、追加の JSON フィールドを処理することなくデータを直接読み取ることができ、JSON 構造を処理する計算オーバーヘッドを削減します。

## Flat JSON 機能の有効化

v3.4 以降では、Flat JSON はデフォルトでグローバルに有効化されています。v3.4 より前のバージョンでは、手動で有効にする必要があります。

v4.0 以降では、この機能はテーブルレベルで設定可能です。

### v3.4 より前のバージョンで有効にする

1. BE 設定を変更します：`enable_json_flat` はバージョン 3.4 以前はデフォルトで `false` です。変更方法については、[Configure BE parameters](../administration/management/BE_configuration.md#configure-be-parameters) を参照してください。
2. FE プルーニング機能を有効化します：

   ```SQL
   SET GLOBAL cbo_prune_json_subfield = true;
   ```

### テーブルレベルでの Flat JSON 機能の有効化

テーブルレベルでの Flat JSON 関連プロパティの設定は、v4.0 以降でサポートされています。

1. テーブルを作成する際、`flat_json.enable` を含む Flat JSON に関連するプロパティを設定できます。詳細な手順については、[CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#テーブルレベルの-flat-json-プロパティを設定) を参照してください。

   または、これらのプロパティを [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) を使用して設定することもできます。

   例：

   ```SQL
   ALTER TABLE t1 SET ("flat_json.enable" = "true");
   ALTER TABLE t1 SET ("flat_json.null.factor" = "0.1");
   ALTER TABLE t1 SET ("flat_json.sparsity.factor" = "0.8");
   ALTER TABLE t1 SET ("flat_json.column.max" = "90");
   ```

2. FE プルーニング機能を有効化します：

   ```SQL
   SET GLOBAL cbo_prune_json_subfield = true;
   ```

## Flat JSON が有効かどうかの確認

データをロードした後、対応する列の抽出されたサブカラムをクエリできます：

```SQL
SELECT flat_json_meta(<json_column>)
FROM <table_name>[_META_];
```

実行されたクエリが Flat JSON 最適化の恩恵を受けているかどうかを [Query Profile](../best_practices/query_tuning/query_profile_overview.md) を通じて以下のメトリクスを観察することで確認できます：
- `PushdownAccessPaths`: ストレージにプッシュダウンされたサブフィールドパスの数。
- `AccessPathHits`: Flat JSON サブフィールドがヒットした回数と、特定の JSON ヒットに関する詳細情報。
- `AccessPathUnhits`: Flat JSON サブフィールドがヒットしなかった回数と、特定の JSON ヒットしなかった情報。
- `JsonFlattern`: Flat JSON がヒットしなかった場合にサブカラムを現場で抽出するのにかかる時間。

## 使用例

1. 機能を有効にする（他のセクションを参照）
2. JSON カラムを持つテーブルを作成します。この例では、INSERT INTO を使用して JSON データをテーブルにロードします。

   ```SQL
    -- method1: JSON カラムを持つテーブルを作成し、作成時に Flat JSON を設定します。これは共有なしクラスタのみをサポートします。
   CREATE TABLE `t1` (
       `k1` int,
       `k2` JSON,
       `k3` VARCHAR(20),
       `k4` JSON
   )             
   DUPLICATE KEY(`k1`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`k1`) BUCKETS 2
   PROPERTIES (
     "replication_num" = "3",
     "flat_json.enable" = "true",
     "flat_json.null.factor" = "0.5",
     "flat_json.sparsity.factor" = "0.5",
     "flat_json.column.max" = "50");
   )
   
   -- method2: Flat JSON 機能を有効にする必要があり、このアプローチは共有なしクラスタと共有データクラスタの両方に適用されます。
   CREATE TABLE `t1` (
       `k1` int,
       `k2` JSON,
       `k3` VARCHAR(20),
       `k4` JSON
   )             
   DUPLICATE KEY(`k1`)
   COMMENT "OLAP"
   DISTRIBUTED BY HASH(`k1`) BUCKETS 2
   PROPERTIES ("replication_num" = "3");   
    
      
   INSERT INTO t1 (k1,k2) VALUES
   (11,parse_json('{"str":"test_flat_json","Integer":123456,"Double":3.14158,"Object":{"c":"d"},"arr":[10,20,30],"Bool":false,"null":null}')),
   (15,parse_json('{"str":"test_str0","Integer":11,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str1","Integer":111,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str2","Integer":222,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (15,parse_json('{"str":"test_str2","Integer":222,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (16,parse_json('{"str":"test_str3","Integer":333,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (17,parse_json('{"str":"test_str3","Integer":333,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (18,parse_json('{"str":"test_str5","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (19,parse_json('{"str":"test_str6","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}')),
   (20,parse_json('{"str":"test_str6","Integer":444,"Double":3.14,"Object":{"a":"b"},"arr":[1,2,3],"Bool":true,"null":null}'));
   ```

3. `k2` カラムの抽出されたサブカラムを表示します。

   ```Plaintext
   SELECT flat_json_meta(k2) FROM t1[_META_];
   +---------------------------------------------------------------------------------------------------------------------------+
   | flat_json_meta(k2)                                                                                                        |
   +---------------------------------------------------------------------------------------------------------------------------+
   | ["nulls(TINYINT)","Integer(BIGINT)","Double(DOUBLE)","str(VARCHAR)","Bool(JSON)","Object(JSON)","arr(JSON)","null(JSON)"] |
   +---------------------------------------------------------------------------------------------------------------------------+
   ```

5. データクエリを実行します。

   ```SQL
   SELECT * FROM t1;
   SELECT get_json_string(k2,'\$.Integer') FROM t1 WHERE k2->'str' = 'test_flat_json';
   SELECT get_json_string(k2,'\$.Double') FROM t1 WHERE k2->'Integer' = 123456;
   SELECT get_json_string(k2,'\$.Object') FROM t1 WHERE k2->'Double' = 3.14158;
   SELECT get_json_string(k2,'\$.arr') FROM t1 WHERE k2->'Object' = to_json(map{'c':'d'});
   SELECT get_json_string(k2,'\$.Bool') FROM t1 WHERE k2->'arr' = '[10,20,30]';
   ```

7. [Query Profile](../best_practices/query_tuning/query_profile_overview.md) で Flat JSON 関連のメトリクスを表示します。
   ```yaml
      PushdownAccessPaths: 2
      - Table: t1
      - AccessPathHits: 2
      - __MAX_OF_AccessPathHits: 1
      - __MIN_OF_AccessPathHits: 1
      - /k2: 2
         - __MAX_OF_/k2: 1
         - __MIN_OF_/k2: 1
      - AccessPathUnhits: 0
      - JsonFlattern: 0ns
   ```

## 関連するセッション変数と設定

### セッション変数

- `cbo_json_v2_rewrite`（デフォルト: true）: JSON v2 のパス書き換えを有効化し、`get_json_*` などの関数を Flat JSON のサブカラムへの直接アクセスに書き換えて、述語プッシュダウンやカラムプルーニングを有効にします。
- `cbo_json_v2_dict_opt`（デフォルト: true）: パス書き換えで生成された Flat JSON の文字列サブカラムに対して、低カーディナリティ辞書最適化を有効にし、文字列式、GROUP BY、JOIN の高速化に寄与します。

例：

```SQL
SET cbo_json_v2_rewrite = true;
SET cbo_json_v2_dict_opt = true;
```

### BE 設定

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)

## 機能の制限

- StarRocks のすべてのテーブルタイプは Flat JSON をサポートします。
- 再インポートを必要とせずに、履歴データと互換性があります。履歴データは Flat JSON によってフラット化されたデータと共存します。
- 履歴データは、新しいデータがロードされるか Compaction が発生しない限り、Flat JSON 最適化を自動的に適用しません。
- Flat JSON を有効にすると、JSON のロードにかかる時間が増加します。抽出される JSON が多いほど、時間がかかります。
- Flat JSON は JSON オブジェクト内の共通キーのみをマテリアライズすることをサポートし、JSON 配列内のキーはサポートしません。
- Flat JSON はデータのソート方法を変更しないため、クエリパフォーマンスとデータ圧縮率はデータのソートによって影響を受け続けます。最適なパフォーマンスを達成するためには、データソートのさらなる調整が必要な場合があります。

## バージョンノート

StarRocks 共有なしクラスタは v3.3.0 から Flat JSON をサポートし、共有データクラスタは v3.3.3 からサポートします。

バージョン v3.3.0、v3.3.1、および v3.3.2 では：
- データをロードする際、共通フィールドを抽出して JSON 型として別々に保存することをサポートし、型推論は行いません。
- 抽出されたカラムと元の JSON データの両方が保存されます。抽出されたデータは元のデータと共に削除されます。

バージョン v3.3.3 から：
- Flat JSON によって抽出された結果は、共通カラムと予約フィールドカラムに分けられます。すべての JSON スキーマが一致する場合、予約フィールドカラムは生成されません。
- Flat JSON は共通フィールドカラムと予約フィールドカラムのみを保存し、元の JSON データを追加で保存しません。
- データをロードする際、共通フィールドは自動的に BIGINT/LARGEINT/DOUBLE/STRING として型推論されます。認識されない型は JSON 型として推論され、予約フィールドカラムは JSON 型として保存されます。
<<<<<<< HEAD

## Flat JSON 機能の有効化（共有なしクラスタのみサポート）

1. テーブル作成時に、`flat_json.enable` プロパティをテーブルパラメータに設定できます。詳細は [Table Creation](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。
   Flat JSON 機能は、テーブルプロパティを直接変更することで有効化または再設定することもできます。例：
   ```SQL
   alter table t1 set ("flat_json.enable" = "true")
   
   alter table t1 set ("flat_json.null.factor" = "0.1")
   
   alter table t1 set ("flat_json.sparsity.factor" = "0.8")
   
   alter table t1 set ("flat_json.column.max" = "90")
   ```
2. FE プルーニング機能を有効化します：`SET GLOBAL cbo_prune_json_subfield = true;`

## Flat JSON 機能の有効化（バージョン 3.4 以前）

1. BE 設定を変更します：`enable_json_flat` はバージョン 3.4 以前はデフォルトで `false` です。変更方法については、[Configure BE parameters](../administration/management/BE_configuration.md#configure-be-parameters) を参照してください。
2. FE プルーニング機能を有効化します：`SET GLOBAL cbo_prune_json_subfield = true;`

## その他のオプションの BE 設定

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)
=======
>>>>>>> 0bbcfda410 ([Doc] Re-organize Flat JSON docs for clarity (#64526))
