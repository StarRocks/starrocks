---
displayed_sidebar: docs
sidebar_position: 80
---

# Add labels on BEs

v3.2.8 以降、StarRocks は BEs にラベルを追加することをサポートしています。テーブルや非同期マテリアライズドビューを作成する際に、特定の BE ノードグループのラベルを指定できます。これにより、データレプリカがそのラベルに関連付けられた BE ノードにのみ分散されることが保証されます。データレプリカは同じラベルを持つノード間で均等に分散され、データの高可用性とリソースの分離が強化されます。

## 使用法

### BEs にラベルを追加する

ある StarRocks クラスターに 6 つの BEs があり、それらが 3 つのラックに均等に分散されているとします。BEs が配置されているラックに基づいて BEs にラベルを追加できます。

```SQL
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.46:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.47:9050" SET ("labels.location" = "rack:rack1");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.48:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.49:9050" SET ("labels.location" = "rack:rack2");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.50:9050" SET ("labels.location" = "rack:rack3");
ALTER SYSTEM MODIFY BACKEND "172.xx.xx.51:9050" SET ("labels.location" = "rack:rack3");
```

ラベルを追加した後、`SHOW BACKENDS;` を実行して、返された結果の `Location` フィールドで BEs のラベルを確認できます。

BEs のラベルを変更する必要がある場合は、`ALTER SYSTEM MODIFY BACKEND "172.xx.xx.48:9050" SET ("labels.location" = "rack:xxx");` を実行できます。

### ラベルを使用して BE ノード上のテーブルデータの分布を指定する

テーブルのデータを分布させる場所を指定する必要がある場合、例えば、テーブルのデータを 2 つのラック、rack1 と rack2 に分布させる場合、テーブルにラベルを追加できます。

ラベルが追加されると、テーブル内の同じ tablet のすべてのレプリカは、ラベル間でラウンドロビン方式で分布されます。さらに、同じラベル内に同じ tablet の複数のレプリカが存在する場合、これらのレプリカはそのラベル内の異なる BEs にできるだけ均等に分布されます。

:::note

- ラベルに関連付けられた BE ノードの総数がレプリカの数より少ない場合、システムは優先的に十分なレプリカを確保します。この場合、レプリカは指定されたラベル通りに分布されないことがあります。
- テーブルに関連付けるラベルは既に存在している必要があります。そうでない場合、エラー `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx` が発生します。

:::

#### テーブル作成時

テーブル作成時に `"labels.location"` プロパティを使用して、テーブルのデータをラック 1 とラック 2 に分布させることができます。

```SQL
CREATE TABLE example_table (
    order_id bigint NOT NULL,
    dt date NOT NULL,
    user_id INT NOT NULL,
    good_id INT NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL
)
PROPERTIES
("labels.location" = "rack:rack1,rack:rack2");
```

新しく作成されたテーブルの場合、テーブルプロパティ `labels.location` のデフォルト値は `*` であり、レプリカがすべてのラベルに均等に分布されることを示しています。新しく作成されたテーブルのデータ分布がクラスター内のサーバーの地理的位置を考慮する必要がない場合、テーブルプロパティ `"labels.location" = ""` を手動で設定できます。

#### テーブル作成後

テーブル作成後にテーブルのデータ分布場所を変更する必要がある場合、例えば、場所をラック 1、ラック 2、ラック 3 に変更する場合、次のステートメントを実行できます。

```SQL
ALTER TABLE example_table
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

StarRocks をバージョン 3.2.8 以降にアップグレードした場合、アップグレード前に作成された履歴テーブルはデフォルトでラベルに基づいて分布されません。履歴テーブルのデータをラベルに基づいて分布させる必要がある場合、次のステートメントを実行して履歴テーブルにラベルを追加できます。

```SQL
ALTER TABLE example_table1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::

### ラベルを使用して BE ノード上のマテリアライズドビューのデータ分布を指定する

非同期マテリアライズドビューのデータを分布させる場所を指定する必要がある場合、例えば、データを 2 つのラック、rack1 と rack2 に分布させる場合、マテリアライズドビューにラベルを追加できます。

ラベルが追加されると、マテリアライズドビュー内の同じ tablet のすべてのレプリカは、ラベル間でラウンドロビン方式で分布されます。さらに、同じラベル内に同じ tablet の複数のレプリカが存在する場合、これらのレプリカはそのラベル内の異なる BEs にできるだけ均等に分布されます。

:::note

- ラベルに関連付けられた BE ノードの総数がレプリカの数より少ない場合、システムは優先的に十分なレプリカを確保します。この場合、レプリカは指定されたラベル通りに分布されないことがあります。
- マテリアライズドビューに関連付けるラベルは既に存在している必要があります。そうでない場合、エラー `Getting analyzing error. Detail message: Cannot find any backend with location: rack:xxx` が発生します。

:::

#### マテリアライズドビュー作成時

マテリアライズドビューのデータをラック 1 とラック 2 に分布させたい場合、次のステートメントを実行できます。

```SQL
CREATE MATERIALIZED VIEW mv_example_mv
DISTRIBUTED BY RANDOM
PROPERTIES (
"labels.location" = "rack:rack1,rack:rack2")
as 
select order_id, dt from example_table;
```

新しく作成されたマテリアライズドビューの場合、プロパティ `labels.location` のデフォルト値は `*` であり、レプリカがすべてのラベルに均等に分布されることを示しています。新しく作成されたマテリアライズドビューのデータ分布がクラスター内のサーバーの地理的位置を考慮する必要がない場合、プロパティ `"labels.location" = ""` を手動で設定できます。

#### マテリアライズドビュー作成後

マテリアライズドビュー作成後にデータ分布場所を変更する必要がある場合、例えば、場所をラック 1、ラック 2、ラック 3 に変更する場合、次のステートメントを実行できます。

```SQL
ALTER MATERIALIZED VIEW mv_example_mv
    SET ("labels.location" = "rack:rack1,rack:rack2,rack:rack3");
```

:::note

StarRocks をバージョン 3.2.8 以降にアップグレードした場合、アップグレード前に作成された既存のマテリアライズドビューはデフォルトでラベルに基づいて分布されません。既存のデータをラベルに基づいて分布させる必要がある場合、次のステートメントを実行してマテリアライズドビューにラベルを追加できます。

```SQL
ALTER TABLE example_mv1
    SET ("labels.location" = "rack:rack1,rack:rack2");
```

:::