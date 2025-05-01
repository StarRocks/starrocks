---
displayed_sidebar: docs
sidebar_position: 30
---

# 重複キーテーブル

重複キーテーブルは、StarRocks のデフォルトモデルです。テーブルを作成する際にモデルを指定しなかった場合、デフォルトで重複キーテーブルが作成されます。

重複キーテーブルを作成する際には、そのテーブルのソートキーを定義することができます。フィルター条件にソートキーの列が含まれている場合、StarRocks はテーブルからデータを迅速にフィルタリングし、クエリを高速化できます。重複キーテーブルでは、新しいデータをテーブルに追加することができますが、既存のデータを変更することはできません。

## シナリオ

重複キーテーブルは、以下のシナリオに適しています：

- 生データの分析、例えば生ログや生の操作記録。
- 事前集計方法に制限されず、さまざまな方法でデータをクエリする。
- ログデータや時系列データのロード。新しいデータは追加専用モードで書き込まれ、既存のデータは更新されません。

## テーブルの作成

特定の時間範囲でイベントデータを分析したいとします。この例では、`detail` という名前のテーブルを作成し、`event_time` と `event_type` をソートキーの列として定義します。

テーブル作成のステートメント：

```SQL
CREATE TABLE IF NOT EXISTS detail (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(user_id);
```

> **注意**
>
> - テーブルを作成する際には、`DISTRIBUTED BY HASH` 句を使用してバケッティング列を指定する必要があります。詳細については、[bucketing](../data_distribution/Data_distribution.md#bucketing) を参照してください。
> - バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。手動でバケット数を設定する必要はありません。詳細については、[バケット数の決定](../data_distribution/Data_distribution.md#determine-the-number-of-buckets) を参照してください。

## 使用上の注意

- テーブルのソートキーについて、以下の点に注意してください：
  - `DUPLICATE KEY` キーワードを使用して、ソートキーに使用される列を明示的に定義できます。

    > 注: デフォルトでは、ソートキーの列を指定しない場合、StarRocks は**最初の3つ**の列をソートキーの列として使用します。

  - 重複キーテーブルでは、ソートキーは一部またはすべてのディメンション列で構成できます。

- テーブル作成時に、ビットマップインデックスやブルームフィルターインデックスなどのインデックスを作成できます。

- 同一のレコードが2つロードされた場合、重複キーテーブルはそれらを1つではなく2つのレコードとして保持します。

## 次のステップ

テーブルが作成された後、さまざまなデータ取り込み方法を使用して StarRocks にデータをロードできます。StarRocks がサポートするデータ取り込み方法については、[データロードの概要](../../loading/Loading_intro.md) を参照してください。
> 注: 重複キーテーブルを使用するテーブルにデータをロードする際には、データをテーブルに追加することしかできません。既存のデータを変更することはできません。