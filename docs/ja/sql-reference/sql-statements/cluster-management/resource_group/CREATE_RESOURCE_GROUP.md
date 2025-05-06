---
displayed_sidebar: docs
---

# CREATE RESOURCE GROUP

## 説明

リソースグループを作成します。

詳細については、 [Resource group](../../../../administration/management/resource_management/resource_group.md) を参照してください。

:::tip

この操作には、SYSTEM レベルの CREATE RESOURCE GROUP 権限が必要です。 [GRANT](../../account-management/GRANT.md) の手順に従って、この権限を付与できます。

:::

## 構文

```SQL
CREATE RESOURCE GROUP resource_group_name 
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## パラメータ

- `resource_group_name`: 作成するリソースグループの名前。

- `CLASSIFIER`: リソース制限が課されるクエリをフィルタリングするために使用されるクラシファイア（分類器）。 `"key"="value"` ペアを使用してクラシファイアを指定する必要があります。リソースグループに対して複数のクラシファイアを設定できます。

  クラシファイアのパラメータは以下の通りです：

    | **パラメータ** | **必須** | **説明**                                              |
    | ------------- | ------------ | ------------------------------------------------------------ |
    | user          | No           | ユーザーの名前。                                            |
    | role          | No           | ユーザーの役割。                                            |
    | query_type    | No           | クエリのタイプ。`SELECT` と `INSERT`（v2.5 から）がサポートされています。`query_type` が `insert` のリソースグループに INSERT タスクがヒットすると、BE ノードはタスクのために指定された CPU リソースを予約します。   |
    | source_ip     | No           | クエリが開始される CIDR ブロック。            |
    | db            | No           | クエリがアクセスするデータベース。カンマ（,）で区切られた文字列で指定できます。 |
    | plan_cpu_cost_range | No     | クエリの推定 CPU コスト範囲。この値は **fe.audit.log** の `PlanCpuCost` フィールドと同等の意味を持ち、単位はありません。形式は `[DOUBLE, DOUBLE)` です。デフォルト値は NULL で、制限がないことを示します。このパラメータは v3.1.4 以降でサポートされています。                  |
    | plan_mem_cost_range | No     | クエリの推定メモリコスト範囲。この値は **fe.audit.log** の `PlanMemCost` フィールドと同等の意味を持ち、単位はありません。形式は `[DOUBLE, DOUBLE)` です。デフォルト値は NULL で、制限がないことを示します。このパラメータは v3.1.4 以降でサポートされています。               |

- `resource_limit`: リソースグループに課されるリソース制限。 `"key"="value"` ペアを使用してリソース制限を指定する必要があります。リソースグループに対して複数のリソース制限を設定できます。

  リソース制限のパラメータは以下の通りです：

    | **パラメータ**              | **必須** | **説明**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | BE 上でリソースグループに割り当てられる CPU コア数のソフトリミット。実際のビジネスシナリオでは、リソースグループに割り当てられる CPU コアは、BE 上の CPU コアの可用性に基づいて比例的にスケールします。有効な値：0 以外の任意の正の整数。 |
    | mem_limit                  | No           | BE が提供する総メモリのうち、クエリに使用できるメモリの割合。単位：%。有効な値：(0, 1)。 |
    | concurrency_limit          | No           | リソースグループ内の同時クエリの上限。過剰な同時クエリによるシステムの過負荷を防ぐために使用されます。 |
    | max_cpu_cores              | No           | 単一の BE ノードでこのリソースグループの CPU コア制限。`0` より大きく設定された場合にのみ有効です。範囲：[0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードにおける平均 CPU コア数を表します。デフォルト：0。 |
    | big_query_cpu_second_limit | No           | 大規模クエリの CPU 占有時間の上限。同時クエリが時間を加算します。単位は秒。 |
    | big_query_scan_rows_limit  | No           | 大規模クエリでスキャンできる行数の上限。 |
    | big_query_mem_limit        | No           | 大規模クエリのメモリ使用量の上限。単位はバイト。 |
    | type                       | No           | リソースグループのタイプ。有効な値：<br />`short_query`: `short_query` リソースグループからのクエリが実行されている場合、BE ノードは `short_query.cpu_core_limit` に定義された CPU コアを予約します。すべての `normal` リソースグループの CPU コアは「総 CPU コア - `short_query.cpu_core_limit`」に制限されます。<br />`normal`: `short_query` リソースグループからのクエリが実行されていない場合、上記の CPU コア制限は `normal` リソースグループには課されません。<br />クラスタ内に `short_query` リソースグループを 1 つだけ作成できます。 |

## 例

例 1: 複数のクラシファイアに基づいてリソースグループ `rg1` を作成します。

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_core_limit' = '10',
      'mem_limit' = '20%',
      'type' = 'normal',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```