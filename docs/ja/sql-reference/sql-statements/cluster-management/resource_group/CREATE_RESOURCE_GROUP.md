---
displayed_sidebar: docs
---

# CREATE RESOURCE GROUP

## 説明

リソースグループを作成します。

詳細については、[Resource group](../../../../administration/management/resource_management/resource_group.md)を参照してください。

:::tip

この操作には、SYSTEMレベルのCREATE RESOURCE GROUP権限が必要です。[GRANT](../../account-management/GRANT.md)の指示に従って、この権限を付与することができます。

:::

## 構文

```SQL
CREATE RESOURCE GROUP resource_group_name 
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## パラメータ

- `resource_group_name`: 作成するリソースグループの名前。

- `CLASSIFIER`: リソース制限が課されるクエリをフィルタリングするために使用されるクラシファイア（分類器）。クラシファイアは`"key"="value"`ペアで指定する必要があります。リソースグループに対して複数のクラシファイアを設定できます。

  クラシファイアのパラメータは以下の通りです：

    | **Parameter** | **Required** | **Description**                                              |
    | ------------- | ------------ | ------------------------------------------------------------ |
    | user          | No           | ユーザーの名前。                                            |
    | role          | No           | ユーザーの役割。                                            |
    | query_type    | No           | クエリのタイプ。`SELECT`と`INSERT`（v2.5から）がサポートされています。`query_type`が`insert`として設定されたリソースグループにINSERTタスクがヒットすると、BEノードはタスクのために指定されたCPUリソースを予約します。   |
    | source_ip     | No           | クエリが開始されるCIDRブロック。            |
    | db            | No           | クエリがアクセスするデータベース。カンマ（,）で区切られた文字列で指定できます。 |
    | plan_cpu_cost_range | No     | クエリの推定CPUコスト範囲。この値は**fe.audit.log**の`PlanCpuCost`フィールドと同等の意味を持ち、単位はありません。形式は`[DOUBLE, DOUBLE)`です。デフォルト値はNULLで、制限がないことを示します。このパラメータはv3.1.4以降でサポートされています。                  |
    | plan_mem_cost_range | No     | クエリの推定メモリコスト範囲。この値は**fe.audit.log**の`PlanMemCost`フィールドと同等の意味を持ち、単位はありません。形式は`[DOUBLE, DOUBLE)`です。デフォルト値はNULLで、制限がないことを示します。このパラメータはv3.1.4以降でサポートされています。               |

- `resource_limit`: リソースグループに課されるリソース制限。リソース制限は`"key"="value"`ペアで指定する必要があります。リソースグループに対して複数のリソース制限を設定できます。

  リソース制限のパラメータは以下の通りです：

    | **Parameter**              | **Required** | **Description**                                              |
    | -------------------------- | ------------ | ------------------------------------------------------------ |
    | cpu_core_limit             | No           | BEでリソースグループに割り当てられるCPUコア数のソフトリミット。実際のビジネスシナリオでは、リソースグループに割り当てられるCPUコアは、BEのCPUコアの可用性に基づいて比例的にスケールします。有効な値：0以外の正の整数。 |
    | cpu_weight                 | No           | 共有リソースグループを作成するために必要なパラメータ。単一のBEノードでのリソースグループのCPUスケジューリングの重みを指定し、このグループからのタスクに割り当てられるCPU時間の相対的なシェアを決定します。実際のビジネスシナリオでは、リソースグループに割り当てられるCPUコアは、BEのCPUコアの可用性に基づいて比例的にスケールします。値の範囲：(0, `avg_be_cpu_cores`]、ここで`avg_be_cpu_cores`はすべてのBEノードの平均CPUコア数です。このパラメータは0より大きい場合にのみ有効です。`cpu_weight`または`exclusive_cpu_cores`のいずれか一方のみが0より大きく設定できます。|
    | exclusive_cpu_cores        | No           | 専用リソースグループ（CPUハードリミット付き）を作成するために必要なパラメータ。`exclusive_cpu_cores` CPUコアをこのリソースグループ専用に予約し、他のグループには利用できないようにし、アイドル時でも他のグループからの利用可能なCPUリソースを使用しないように制限します。値の範囲：(0, `min_be_cpu_cores - 1`]、ここで`min_be_cpu_cores`はすべてのBEノードの最小CPUコア数です。0より大きい場合にのみ有効です。`cpu_weight`または`exclusive_cpu_cores`のいずれか一方のみが0より大きく設定できます。|
    | mem_limit                  | No           | BEが提供する総メモリのうち、クエリに使用できるメモリの割合。単位：%。有効な値：(0, 1)。 |
    | concurrency_limit          | No           | リソースグループ内の同時クエリの上限。過剰な同時クエリによるシステムの過負荷を防ぐために使用されます。 |
    | max_cpu_cores              | No           | 単一のBEノードでのこのリソースグループのCPUコア制限。`0`より大きい場合にのみ有効です。範囲：[0, `avg_be_cpu_cores`]、ここで`avg_be_cpu_cores`はすべてのBEノードの平均CPUコア数を表します。デフォルト：0。 |
    | big_query_cpu_second_limit | No           | 大規模クエリのCPU占有時間の上限。同時クエリが時間を加算します。単位は秒です。 |
    | big_query_scan_rows_limit  | No           | 大規模クエリによってスキャンされる行数の上限。 |
    | big_query_mem_limit        | No           | 大規模クエリのメモリ使用量の上限。単位はバイトです。 |

    > **NOTE**
    >
    > v3.3.5以前は、StarRocksはリソースグループの`type`を`short_query`に設定することを許可していました。しかし、パラメータ`type`は廃止され、`exclusive_cpu_cores`に置き換えられました。このタイプの既存のリソースグループについては、システムはv3.3.5にアップグレードした後、`exclusive_cpu_cores`の値が`cpu_weight`と等しい専用リソースグループに自動的に変換します。

## 例

例1: 複数のクラシファイアに基づいて共有リソースグループ`rg1`を作成します。

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

例2: 複数のクラシファイアに基づいて専用リソースグループ`rg2`を作成します。

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