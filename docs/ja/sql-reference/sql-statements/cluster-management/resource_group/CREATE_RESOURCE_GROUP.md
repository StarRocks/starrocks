---
displayed_sidebar: docs
---

# CREATE RESOURCE GROUP

## 説明

リソースグループを作成します。

詳細については、 [Resource group](../../../../administration/management/resource_management/resource_group.md) を参照してください。

:::tip

この操作には、SYSTEM レベルの CREATE RESOURCE GROUP 権限が必要です。この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```SQL
CREATE RESOURCE GROUP resource_group_name 
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## パラメータ

- `resource_group_name`: 作成するリソースグループの名前。

- `CLASSIFIER`: リソース制限が課されるクエリをフィルタリングするために使用されるクラシファイア（分類器）。クラシファイアは `"key"="value"` のペアで指定する必要があります。リソースグループには複数のクラシファイアを設定できます。

  クラシファイアのパラメータは以下の通りです：

    | **パラメータ** | **必須** | **説明**                                              |
    | ------------- | ------------ | ------------------------------------------------------------ |
    | user          | No           | ユーザーの名前。                                            |
    | role          | No           | ユーザーの役割。                                            |
    | query_type    | No           | クエリのタイプ。`SELECT` と `INSERT`（v2.5から）がサポートされています。`query_type` が `insert` のリソースグループに INSERT タスクがヒットすると、BE ノードはタスクのために指定された CPU リソースを予約します。   |
    | source_ip     | No           | クエリが開始される CIDR ブロック。            |
    | db            | No           | クエリがアクセスするデータベース。カンマ（,）で区切られた文字列で指定できます。 |
    | plan_cpu_cost_range | No     | クエリの推定 CPU コスト範囲。その値は **fe.audit.log** のフィールド `PlanCpuCost` と同等の意味を持ち、単位はありません。形式は `[DOUBLE, DOUBLE)` です。デフォルト値は NULL で、そのような制限がないことを示します。このパラメータは v3.1.4 以降でサポートされています。                  |
    | plan_mem_cost_range | No     | クエリの推定メモリコスト範囲。その値は **fe.audit.log** のフィールド `PlanMemCost` と同等の意味を持ち、単位はありません。形式は `[DOUBLE, DOUBLE)` です。デフォルト値は NULL で、そのような制限がないことを示します。このパラメータは v3.1.4 以降でサポートされています。               |

- `resource_limit`: リソースグループに課されるリソース制限。リソース制限は `"key"="value"` のペアで指定する必要があります。リソースグループには複数のリソース制限を設定できます。

  リソース制限のパラメータは以下の通りです：

    | **パラメータ**              | **必須** | **説明**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | BE 上でリソースグループに割り当てられる CPU コア数のソフトリミット。実際のビジネスシナリオでは、リソースグループに割り当てられる CPU コアは BE 上の CPU コアの可用性に基づいて比例的にスケールします。有効な値：0 以外の正の整数。 |
    | cpu_weight                 | No           | Shared リソースグループを作成するために必要なパラメータです。単一の BE ノード上でのリソースグループの CPU スケジューリングの重みを指定し、このグループからのタスクに割り当てられる CPU 時間の相対的なシェアを決定します。実際のビジネスシナリオでは、リソースグループに割り当てられる CPU コアは BE 上の CPU コアの可用性に基づいて比例的にスケールします。値の範囲：(0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数です。このパラメータは 0 より大きい場合にのみ有効です。`cpu_weight` または `exclusive_cpu_cores` のいずれか一方のみが 0 より大きく設定できます。|
    | exclusive_cpu_cores        | No           | Exclusive リソースグループ（CPU ハードリミット付き）を作成するために必要なパラメータです。このリソースグループのために `exclusive_cpu_cores` CPU コアを専用に予約し、アイドル時でも他のグループには利用できないようにし、リソースグループがこれらの予約された CPU コアのみを使用するように制限し、他のグループから利用可能な CPU リソースを使用できないようにします。値の範囲：(0, `min_be_cpu_cores - 1`]、ここで `min_be_cpu_cores` はすべての BE ノードの最小 CPU コア数です。0 より大きい場合にのみ有効です。`cpu_weight` または `exclusive_cpu_cores` のいずれか一方のみが 0 より大きく設定できます。|
    | mem_limit                  | No           | BE が提供する総メモリのうち、クエリに使用できるメモリの割合。単位：%。有効な値：(0, 1)。 |
    | concurrency_limit          | No           | リソースグループ内の同時クエリの上限。過剰な同時クエリによるシステムの過負荷を防ぐために使用されます。 |
    | max_cpu_cores              | No           | 単一の BE ノード上でのこのリソースグループの CPU コア制限。`0` より大きい場合にのみ有効です。範囲：[0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数を表します。デフォルト：0。 |
    | big_query_cpu_second_limit | No           | 大規模クエリの CPU 占有時間の上限。並行クエリは時間を加算します。単位は秒です。 |
    | big_query_scan_rows_limit  | No           | 大規模クエリでスキャンできる行数の上限。 |
    | big_query_mem_limit        | No           | 大規模クエリのメモリ使用量の上限。単位はバイトです。 |

    > **NOTE**
    >
    > v3.3.5 より前は、StarRocks はリソースグループの `type` を `short_query` に設定することを許可していました。しかし、パラメータ `type` は廃止され、`exclusive_cpu_cores` に置き換えられました。このタイプの既存のリソースグループについては、システムは v3.3.5 にアップグレードした後、自動的に `exclusive_cpu_cores` の値が `cpu_weight` に等しい Exclusive リソースグループに変換します。

## 例

例 1: 複数のクラシファイアに基づいて Shared リソースグループ `rg1` を作成します。

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_weight' = '10',
      'mem_limit' = '20%',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```

例 2: 複数のクラシファイアに基づいて Exclusive リソースグループ `rg2` を作成します。

```SQL
CREATE RESOURCE GROUP rg2
TO 
    (user='rg1_user5', role='rg1_role5', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user6', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user7', source_ip='192.168.x.x/24'),
    (user='rg1_user8'),
    (db='db2')
WITH ('exclusive_cpu_cores' = '10',
      'mem_limit' = '20%',
      'type' = 'normal',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```