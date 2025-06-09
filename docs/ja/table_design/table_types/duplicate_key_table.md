---
displayed_sidebar: docs
sidebar_position: 30
---

# 重複キーテーブル

重複キーテーブルは、StarRocks のデフォルトモデルです。テーブルを作成する際にモデルを指定しない場合、デフォルトで重複キーテーブルが作成されます。

重複キーテーブルを作成する際には、そのテーブルに対してソートキーを定義することができます。フィルター条件にソートキーの列が含まれている場合、StarRocks はテーブルからデータを迅速にフィルタリングし、クエリを高速化できます。

重複キーテーブルは、ログデータの分析などのシナリオに適しています。新しいデータの追加はサポートしていますが、履歴データの修正はサポートしていません。

## シナリオ

重複キーテーブルは、以下のシナリオに適しています:

- 生データの分析、例えば生のログや生の操作記録。
- 事前集計方法に制限されず、様々な方法でデータをクエリする。
- ログデータや時系列データのロード。新しいデータは追加専用モードで書き込まれ、既存のデータは更新されません。

## テーブルの作成

特定の時間範囲にわたるイベントデータを分析したいとします。この例では、`detail` という名前のテーブルを作成し、`event_time` と `event_type` をソートキーの列として定義します。

テーブル作成のステートメント:

```SQL
CREATE TABLE detail (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT "")
ORDER BY (event_time, event_type);
```

## 使用上の注意

- **ソートキー**

  v3.3.0 以降、重複キーテーブルは `ORDER BY` を使用してソートキーを指定することをサポートしています。これは任意の列の組み合わせにすることができます。`ORDER BY` と `DUPLICATE KEY` の両方が使用されている場合、`DUPLICATE KEY` は効果を持ちません。`ORDER BY` も `DUPLICATE KEY` も使用されていない場合、デフォルトでテーブルの最初の3列がソートキーとして使用されます。

- **バケッティング**

  - **バケッティング方法**:

  v3.1.0 以降、StarRocks は重複キーテーブルに対してランダムバケット法をサポートしています（デフォルトのバケッティング方法）。テーブルを作成する際やパーティションを追加する際に、ハッシュバケッティングキー（`DISTRIBUTED BY HASH` 句）は設定する必要がありません。v3.1.0 より前は、StarRocks はハッシュバケッティングのみをサポートしていました。テーブルを作成する際やパーティションを追加する際に、ハッシュバケッティングキー（`DISTRIBUTED BY HASH` 句）を設定する必要がありました。設定しないと、テーブルの作成に失敗します。ハッシュバケッティングキーの詳細については、[Hash Bucketing](../data_distribution/Data_distribution.md#hash-bucketing) を参照してください。

  - **バケット数**: v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数（BUCKETS）を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[set the number of buckets](../data_distribution/Data_distribution.md#set-the-number-of-buckets) を参照してください。

- テーブルを作成する際、テーブルのすべての列に対して Bitmap インデックスや Bloom Filter インデックスを作成することができます。

## 次のステップ

テーブルが作成された後、様々なデータ取り込み方法を使用して StarRocks にデータをロードできます。StarRocks がサポートするデータ取り込み方法については、[Loading options](../../loading/Loading_intro.md) を参照してください。

:::note

- 重複キーテーブルを使用するテーブルにデータをロードする際、テーブルにデータを追加することしかできません。テーブル内の既存データを修正することはできません。
- 同一のレコードが2つロードされた場合、重複キーテーブルはそれらを1つではなく2つのレコードとして保持します。

:::