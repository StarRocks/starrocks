---
displayed_sidebar: docs
sidebar_position: 110
---

# [プレビュー] Flat JSON

この記事では、Flat JSON の基本概念とこの機能の使い方を紹介します。

バージョン 2.2.0 から、StarRocks は JSON データ型をサポートし、より柔軟なデータストレージを可能にしています。しかし、JSON をクエリする際、多くのシナリオでは JSON データ全体を直接読み込むのではなく、指定されたパスのデータにアクセスすることが一般的です。例えば:

```SQL
-- 必要なフィールドをログに固定フィールドとして保存し、ビジネスに応じて頻繁に変わる他のフィールドを JSON としてパッケージ化します。
SELECT
    time,
    event,
    user,
    get_json_string(remain_json, "$.from_system"),
    get_json_string(remain_json, "$.tag")
FROM logs;
```

JSON 型の特性上、クエリにおけるパフォーマンスは標準型（INT、STRING など）ほど良くありません。その理由は以下の通りです:
- ストレージのオーバーヘッド: JSON は半構造化型であり、各行の構造情報を保存する必要があるため、ストレージ使用量が多く、圧縮効率が低いです。
- クエリの複雑さ: クエリは実行時データに基づいてデータ構造を検出する必要があり、ベクトル化実行の最適化を達成するのが難しいです。
- 冗長データ: クエリは多くの冗長フィールドを含む JSON データ全体を読み込む必要があります。

StarRocks は JSON データのクエリ効率を向上させ、JSON の使用の複雑さを軽減するために Flat JSON 機能を導入しました。
- この機能はバージョン 3.3.0 から利用可能で、デフォルトでは無効になっており、手動で有効にする必要があります。
- バージョン 3.4.0 からは、Flat JSON 機能がデフォルトで有効になり、手動操作は不要です。

## Flat JSON とは

Flat JSON の核心原理は、ロード中に JSON データを検出し、JSON データから共通フィールドを抽出して標準型データとして保存することです。JSON をクエリする際、これらの共通フィールドが JSON のクエリ速度を最適化します。例として以下のデータがあります:

```Plaintext
1, {"a": 1, "b": 21, "c": 3, "d": 4}
2, {"a": 2, "b": 22, "d": 4}
3, {"a": 3, "b": 23, "d": [1, 2, 3, 4]}
4, {"a": 4, "b": 24, "d": null}
5, {"a": 5, "b": 25, "d": null}
6, {"c": 6, "d": 1}
```

上記の JSON データをロードする際、フィールド `a` と `b` はほとんどの JSON データに存在し、データ型が類似しています（どちらも INT）。したがって、フィールド `a` と `b` のデータは JSON から抽出され、2 つの INT 列として別々に保存されます。これらの 2 列がクエリで使用されるとき、そのデータは直接読み取られ、追加の JSON フィールドを処理する必要がなくなり、JSON 構造を扱う計算オーバーヘッドが削減されます。

## Flat JSON が有効かどうかを確認する

データをロードした後、対応する列の抽出されたサブカラムをクエリできます:

```SQL
SELECT flat_json_meta(<json_column>)
FROM <table_name>[_META_];
```

実行されたクエリが Flat JSON 最適化の恩恵を受けているかどうかは、[Query Profile](../administration/query_profile_overview.md) を通じて以下のメトリクスを観察することで確認できます:
- `PushdownAccessPaths`: ストレージにプッシュダウンされたサブフィールドパスの数。
- `AccessPathHits`: Flat JSON サブフィールドがヒットした回数と、特定の JSON ヒットに関する詳細情報。
- `AccessPathUnhits`: Flat JSON サブフィールドがヒットしなかった回数と、特定の JSON がヒットしなかったことに関する詳細情報。
- `JsonFlattern`: Flat JSON がヒットしなかった場合に、現場でサブカラムを抽出するのにかかる時間。

## 使用例

1. 機能を有効にする（他のセクションを参照）
2. JSON 列を持つテーブルを作成します。この例では、INSERT INTO を使用して JSON データをテーブルにロードします。

   ```SQL
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

3. `k2` 列の抽出されたサブカラムを表示します。

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

7. [Query Profile](../administration/query_profile_overview.md) で Flat JSON に関連するメトリクスを表示します。
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

## 機能の制限

- StarRocks のすべてのテーブルタイプは Flat JSON をサポートしています。
- 再インポートを必要とせず、既存のデータと互換性があります。既存のデータは Flat JSON によってフラット化されたデータと共存します。
- 既存のデータは、新しいデータがロードされるか、Compaction が発生しない限り、Flat JSON 最適化を自動的に適用しません。
- Flat JSON を有効にすると、JSON のロードにかかる時間が増加します。抽出される JSON が多いほど、時間がかかります。
- Flat JSON は JSON オブジェクト内の共通キーのみをマテリアライズすることができ、JSON 配列内のキーはサポートしません。
- Flat JSON はデータのソート方法を変更しないため、クエリパフォーマンスとデータ圧縮率はデータのソートによって影響を受けます。最適なパフォーマンスを達成するためには、データソートのさらなる調整が必要な場合があります。

## バージョンノート

StarRocks 共有なしクラスタは v3.3.0 から Flat JSON をサポートし、共有データクラスタは v3.3.3 からサポートしています。

バージョン v3.3.0、v3.3.1、v3.3.2 では:
- データをロードする際、共通フィールドを抽出し、型推論なしで JSON 型として別々に保存することをサポートします。
- 抽出された列と元の JSON データの両方が保存されます。抽出されたデータは元のデータとともに削除されます。

バージョン v3.3.3 から:
- Flat JSON によって抽出された結果は、共通カラムと予約フィールドカラムに分けられます。すべての JSON スキーマが一致している場合、予約フィールドカラムは生成されません。
- Flat JSON は共通フィールドカラムと予約フィールドカラムのみを保存し、元の JSON データを追加で保存しません。
- データをロードする際、共通フィールドは自動的に BIGINT/LARGEINT/DOUBLE/STRING として型推論されます。認識されない型は JSON 型として推論され、予約フィールドカラムは JSON 型として保存されます。

## Flat JSON 機能を有効にする（バージョン 3.4 以前）

1. BE 設定を変更します: `enable_json_flat`、バージョン 3.4 以前はデフォルトで `false` です。変更方法については、[BE パラメータの設定](../administration/management/BE_configuration.md#configure-be-parameters) を参照してください。
2. FE プルーニング機能を有効にします: `SET GLOBAL cbo_prune_json_subfield = true;`

## その他のオプションの BE 設定

- [json_flat_null_factor](../administration/management/BE_configuration.md#json_flat_null_factor)
- [json_flat_column_max](../administration/management/BE_configuration.md#json_flat_column_max)
- [json_flat_sparsity_factor](../administration/management/BE_configuration.md#json_flat_sparsity_factor)
- [enable_compaction_flat_json](../administration/management/BE_configuration.md#enable_compaction_flat_json)
- [enable_lazy_dynamic_flat_json](../administration/management/BE_configuration.md#enable_lazy_dynamic_flat_json)