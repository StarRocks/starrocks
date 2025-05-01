---
displayed_sidebar: docs
---

# CREATE RESOURCE GROUP

## 説明

リソースグループを作成します。

詳細については、 [Resource group](../../../../administration/management/resource_management/resource_group.md) を参照してください。

:::tip

この操作には、SYSTEMレベルのCREATE RESOURCE GROUP権限が必要です。 この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```SQL
CREATE RESOURCE GROUP resource_group_name 
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## パラメータ

- `resource_group_name`: 作成するリソースグループの名前。

- `CLASSIFIER`: リソース制限が課されるクエリをフィルタリングするために使用されるクラシファイア（分類器）。 `"key"="value"` ペアを使用してクラシファイアを指定する必要があります。 リソースグループに対して複数のクラシファイアを設定できます。

  クラシファイアのパラメータは以下の通りです：

    | **Parameter** | **Required** | **Description**                                              |
    | ------------- | ------------ | ------------------------------------------------------------ |
    | user          | No           | ユーザーの名前。                                            |
    | role          | No           | ユーザーの役割。                                            |
    | query_type    | No           | クエリのタイプ。 `SELECT` と `INSERT`（v2.5から）がサポートされています。 `query_type` が `insert` のリソースグループにINSERTタスクがヒットすると、BEノードはタスクのために指定されたCPUリソースを予約します。   |
    | source_ip     | No           | クエリが開始されるCIDRブロック。            |
    | db            | No           | クエリがアクセスするデータベース。カンマ（,）で区切られた文字列で指定できます。 |
    | plan_cpu_cost_range | No     | クエリの推定CPUコスト範囲。その値は **fe.audit.log** の `PlanCpuCost` フィールドと同等の意味を持ち、単位はありません。フォーマットは `[DOUBLE, DOUBLE)` です。デフォルト値はNULLで、そのような制限がないことを示します。このパラメータはv3.1.4以降でサポートされています。                  |
    | plan_mem_cost_range | No     | クエリの推定メモリコスト範囲。その値は **fe.audit.log** の `PlanMemCost` フィールドと同等の意味を持ち、単位はありません。フォーマットは `[DOUBLE, DOUBLE)` です。デフォルト値はNULLで、そのような制限がないことを示します。このパラメータはv3.1.4以降でサポートされています。               |

- `resource_limit`: リソースグループに課されるリソース制限。 `"key"="value"` ペアを使用してリソース制限を指定する必要があります。 リソースグループに対して複数のリソース制限を設定できます。

  リソース制限のパラメータは以下の通りです：

    | **Parameter**              | **Required** | **Description**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | BE上でリソースグループに割り当てられるCPUコア数のソフトリミット。実際のビジネスシナリオでは、リソースグループに割り当てられるCPUコアは、BE上のCPUコアの可用性に基づいて比例的にスケールします。有効な値：0以外の正の整数。 |
    | mem_limit                  | No           | BEが提供する総メモリの中で、クエリに使用できるメモリの割合。単位：%。有効な値：(0, 1)。 |
    | concurrency_limit          | No           | リソースグループ内の同時クエリの上限。過剰な同時クエリによるシステムの過負荷を防ぐために使用されます。 |
    | max_cpu_cores              | No           | 単一のBEノードでのこのリソースグループのCPUコア制限。`0`より大きく設定された場合にのみ有効です。範囲：[0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべてのBEノードの平均CPUコア数を表します。デフォルト：0。 |
    | big_query_cpu_second_limit | No           | 大規模クエリのCPU占有の上限時間。同時クエリが時間を加算します。単位は秒。 |
    | big_query_scan_rows_limit  | No           | 大規模クエリがスキャンできる行数の上限。 |
    | big_query_mem_limit        | No           | 大規模クエリのメモリ使用量の上限。単位はバイト。 |
    | type                       | No           | リソースグループのタイプ。有効な値： <br />`short_query`: `short_query` リソースグループからのクエリが実行されている場合、BEノードは `short_query.cpu_core_limit` に定義されたCPUコアを予約します。すべての `normal` リソースグループのCPUコアは「総CPUコア - `short_query.cpu_core_limit`」に制限されます。 <br />`normal`: `short_query` リソースグループからのクエリが実行されていない場合、上記のCPUコア制限は `normal` リソースグループに課されません。 <br />注意：クラスター内に `short_query` リソースグループを1つだけ作成できます。 |

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