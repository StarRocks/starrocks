---
displayed_sidebar: docs
---

# Resource group

このトピックでは、StarRocks のリソースグループ機能について説明します。

![resource group](../_assets/resource_group.png)

この機能を使用すると、短いクエリ、アドホッククエリ、ETL ジョブなど、複数のワークロードを単一のクラスターで同時に実行でき、複数のクラスターを展開するための追加コストを節約できます。技術的な観点から見ると、実行エンジンはユーザーの指定に従って同時にワークロードをスケジュールし、それらの間の干渉を隔離します。

リソースグループ機能のロードマップ：

- v2.2 以降、StarRocks はクエリのリソース消費を制限し、同じクラスター内のテナント間でのリソースの隔離と効率的な使用を実現します。
- StarRocks v2.3 では、大規模クエリのリソース消費をさらに制限し、クエリ要求が過大になってクラスターリソースが枯渇するのを防ぎ、システムの安定性を保証します。
- StarRocks v2.5 では、データロード（INSERT）の計算リソース消費を制限することができます。

|  | Internal Table | External Table | Big Query Restriction| Short Query | Data Ingestion  | Schema Change | INSERT |
|---|---|---|---|---|---|---|---|
| 2.2 | √ | × | × | × | × | × | × |
| 2.3 | √ | √ | √ | √ | × | × | × |
| 2.4 | √ | √ | √ | √ | × | × | × |
| 2.5 | √ | √ | √ | √ | √ | × | √ |

## Terms

このセクションでは、リソースグループ機能を使用する前に理解しておくべき用語について説明します。

### resource group

各リソースグループは、特定の BE からの計算リソースのセットです。クラスターの各 BE を複数のリソースグループに分割できます。クエリがリソースグループに割り当てられると、StarRocks は指定されたリソースクォータに基づいてリソースグループに CPU とメモリリソースを割り当てます。

BE 上のリソースグループに対して CPU とメモリのリソースクォータを指定するには、次のパラメータを使用します：

- `cpu_core_limit`

  このパラメータは、BE 上のリソースグループに割り当てられる CPU コア数のソフトリミットを指定します。有効な値：0 以外の正の整数。

  実際のビジネスシナリオでは、リソースグループに割り当てられる CPU コアは、BE 上の CPU コアの可用性に基づいて比例的にスケールします。

  > **NOTE**
  >
  > 例えば、16 CPU コアを提供する BE に 3 つのリソースグループを構成します：rg1、rg2、および rg3。3 つのリソースグループの `cpu_core_limit` の値はそれぞれ `2`、`6`、`8` です。
  >
  > BE のすべての CPU コアが占有されている場合、3 つのリソースグループそれぞれに割り当てられる CPU コアの数は、次の計算に基づいてそれぞれ 2、6、8 です：
  >
  > - rg1 の CPU コア数 = BE 上の CPU コアの総数 × (2/16) = 2
  > - rg2 の CPU コア数 = BE 上の CPU コアの総数 × (6/16) = 6
  > - rg3 の CPU コア数 = BE 上の CPU コアの総数 × (8/16) = 8
  >
  > BE のすべての CPU コアが占有されていない場合、例えば rg1 と rg2 がロードされているが rg3 がロードされていない場合、rg1 と rg2 に割り当てられる CPU コアの数は次の計算に基づいてそれぞれ 4 と 12 です：
  >
  > - rg1 の CPU コア数 = BE 上の CPU コアの総数 × (2/8) = 4
  > - rg2 の CPU コア数 = BE 上の CPU コアの総数 × (6/8) = 12

- `mem_limit`

  このパラメータは、BE が提供する総メモリのうち、クエリに使用できるメモリの割合を指定します。有効な値：(0, 1)。

  > **NOTE**
  >
  > クエリに使用できるメモリの量は `query_pool` パラメータで示されます。パラメータの詳細については、[Memory management](Memory_management.md) を参照してください。

- `concurrency_limit`

  このパラメータは、リソースグループ内の同時クエリの上限を指定します。これは、同時クエリが多すぎてシステムが過負荷になるのを防ぐために使用されます。このパラメータは、0 より大きい値に設定された場合にのみ有効です。デフォルト：0。

上記のリソース消費制限に基づいて、次のパラメータを使用して大規模クエリのリソース消費をさらに制限できます：

- `big_query_cpu_second_limit`: このパラメータは、大規模クエリの CPU 占有時間の上限を指定します。同時クエリは時間を合計します。単位は秒です。このパラメータは、0 より大きい値に設定された場合にのみ有効です。デフォルト：0。
- `big_query_scan_rows_limit`: このパラメータは、大規模クエリがスキャンできる行数の上限を指定します。このパラメータは、0 より大きい値に設定された場合にのみ有効です。デフォルト：0。
- `big_query_mem_limit`: このパラメータは、大規模クエリのメモリ使用量の上限を指定します。単位はバイトです。このパラメータは、0 より大きい値に設定された場合にのみ有効です。デフォルト：0。

> **NOTE**
>
> リソースグループで実行中のクエリが上記の大規模クエリ制限を超えた場合、クエリはエラーで終了します。エラーメッセージは FE ノードの `ErrorCode` 列 **fe.audit.log** でも確認できます。

リソースグループの `type` を `short_query` または `normal` に設定できます。

- デフォルト値は `normal` です。パラメータ `type` に `normal` を指定する必要はありません。
- クエリが `short_query` リソースグループにヒットすると、BE ノードは `short_query.cpu_core_limit` で指定された CPU リソースを予約します。`normal` リソースグループにヒットするクエリに予約される CPU リソースは `BE core - short_query.cpu_core_limit` に制限されます。
- クエリが `short_query` リソースグループにヒットしない場合、`normal` リソースグループのリソースには制限が課されません。

> **CAUTION**
>
> - StarRocks クラスターでは、最大で 1 つの short query リソースグループを作成できます。
> - StarRocks は `short_query` リソースグループの CPU リソースに対してハード上限を設定していません。

### クラシファイア（分類器）

各クラシファイアは、クエリのプロパティに一致する 1 つ以上の条件を保持します。StarRocks は、各クエリに最も一致するクラシファイアを条件に基づいて特定し、クエリを実行するためのリソースを割り当てます。

クラシファイアは次の条件をサポートします：

- `user`: ユーザーの名前。
- `role`: ユーザーの役割。
- `query_type`: クエリのタイプ。`SELECT` と `INSERT`（v2.5 から）がサポートされています。INSERT タスクが `query_type` を `insert` とするリソースグループにヒットすると、BE ノードはタスクのために指定された CPU リソースを予約します。
- `source_ip`: クエリが開始される CIDR ブロック。
- `db`: クエリがアクセスするデータベース。カンマ `,` で区切られた文字列で指定できます。

クラシファイアは、クラシファイアの条件のいずれかまたはすべてがクエリの情報に一致する場合にのみクエリに一致します。複数のクラシファイアがクエリに一致する場合、StarRocks はクエリと各クラシファイアの一致度を計算し、一致度が最も高いクラシファイアを特定します。

> **NOTE**
>
> クエリが属するリソースグループは、FE ノードの `ResourceGroup` 列 **fe.audit.log** で確認できます。
>
> クエリがクラシファイアにヒットしない場合、デフォルトのリソースグループ `default_wg` が使用されます。`default_wg` のリソース制限は次のとおりです：
>
> - `cpu_core_limit`: 1 (`<=` v2.3.7) または BE の CPU コア数 (`>` v2.3.7)
> - `mem_limit`: 100%
> - `concurrency_limit`: 0
> - `big_query_cpu_second_limit`: 0
> - `big_query_scan_rows_limit`: 0
> - `big_query_mem_limit`: 0

StarRocks は、次のルールを使用してクエリとクラシファイアの一致度を計算します：

- クラシファイアがクエリと同じ `user` 値を持つ場合、クラシファイアの一致度は 1 増加します。
- クラシファイアがクエリと同じ `role` 値を持つ場合、クラシファイアの一致度は 1 増加します。
- クラシファイアがクエリと同じ `query_type` 値を持つ場合、クラシファイアの一致度は 1 増加し、次の計算から得られる数値が加算されます：1/クラシファイアの `query_type` フィールドの数。
- クラシファイアがクエリと同じ `source_ip` 値を持つ場合、クラシファイアの一致度は 1 増加し、次の計算から得られる数値が加算されます：(32 - `cidr_prefix`)/64。
- クラシファイアがクエリと同じ `db` 値を持つ場合、クラシファイアの一致度は 10 増加します。

複数のクラシファイアがクエリに一致する場合、条件の数が多いクラシファイアの方が一致度が高くなります。

```plaintext
-- クラシファイア B はクラシファイア A よりも多くの条件を持っています。したがって、クラシファイア B はクラシファイア A よりも一致度が高くなります。

classifier A (user='Alice')

classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

複数の一致するクラシファイアが同じ数の条件を持つ場合、条件がより正確に記述されているクラシファイアの方が一致度が高くなります。

```plaintext
-- クラシファイア B に指定された CIDR ブロックは、クラシファイア A よりも範囲が小さいです。したがって、クラシファイア B はクラシファイア A よりも一致度が高くなります。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- クラシファイア C はクラシファイア D よりも指定されたクエリタイプが少ないです。したがって、クラシファイア C はクラシファイア D よりも一致度が高くなります。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select'))
```

## 計算リソースの隔離

リソースグループとクラシファイアを構成することで、クエリ間の計算リソースを隔離できます。

### リソースグループを有効にする

リソースグループを有効にするには、次のステートメントを実行します：

```SQL
SET enable_resource_group = true;
```

リソースグループをグローバルに有効にするには、次のステートメントを実行します：

```SQL
SET GLOBAL enable_resource_group = true;
```

### リソースグループとクラシファイアを作成する

リソースグループを作成し、クラシファイアと関連付け、リソースグループに計算リソースを割り当てるには、次のステートメントを実行します：

```SQL
CREATE RESOURCE GROUP <group_name> 
TO (
    user='string', 
    role='string', 
    query_type in ('select'), 
    source_ip='cidr'
) --クラシファイアを作成します。複数のクラシファイアを作成する場合は、クラシファイアをカンマ（`,`）で区切ります。
WITH (
    "cpu_core_limit" = "INT",
    "mem_limit" = "m%",
    "concurrency_limit" = "INT",
    "type" = "str" --リソースグループのタイプ。値を normal に設定します。
);
```

例：

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '100000',
    'big_query_mem_limit' = '1073741824'
);
```

### リソースグループを指定する（オプション）

現在のセッションに対してリソースグループを直接指定できます。

```SQL
SET resource_group = 'group_name';
```

### リソースグループとクラシファイアを表示する

すべてのリソースグループとクラシファイアをクエリするには、次のステートメントを実行します：

```SQL
SHOW RESOURCE GROUPS ALL;
```

ログインしているユーザーのリソースグループとクラシファイアをクエリするには、次のステートメントを実行します：

```SQL
SHOW RESOURCE GROUPS;
```

指定されたリソースグループとそのクラシファイアをクエリするには、次のステートメントを実行します：

```SQL
SHOW RESOURCE GROUP group_name;
```

例：

```plain
mysql> SHOW RESOURCE GROUPS ALL;
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> **NOTE**
>
> 上記の例では、`weight` は一致度を示します。

### リソースグループとクラシファイアを管理する

各リソースグループのリソースクォータを変更できます。また、リソースグループからクラシファイアを追加または削除することもできます。

既存のリソースグループのリソースクォータを変更するには、次のステートメントを実行します：

```SQL
ALTER RESOURCE GROUP group_name WITH (
    'cpu_core_limit' = 'INT',
    'mem_limit' = 'm%'
);
```

リソースグループを削除するには、次のステートメントを実行します：

```SQL
DROP RESOURCE GROUP group_name;
```

リソースグループにクラシファイアを追加するには、次のステートメントを実行します：

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

リソースグループからクラシファイアを削除するには、次のステートメントを実行します：

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...);
```

リソースグループのすべてのクラシファイアを削除するには、次のステートメントを実行します：

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

## リソースグループを監視する

リソースグループに対して [monitor and alert](Monitor_and_Alert.md) を設定できます。

リソースグループに関して監視できるメトリクスには以下が含まれます：

- FE
  - `starrocks_fe_query_resource_group`: 各リソースグループのクエリ数。
  - `starrocks_fe_query_resource_group_latency`: 各リソースグループのクエリ遅延パーセンタイル。
  - `starrocks_fe_query_resource_group_err`: 各リソースグループでエラーで終了したクエリの数。
- BE
  - `starrocks_be_resource_group_cpu_limit_ratio`: リソースグループの CPU クォータ比率の瞬時値。
  - `starrocks_be_resource_group_cpu_use_ratio`: リソースグループの CPU 使用率の瞬時値。
  - `starrocks_be_resource_group_mem_limit_bytes`: リソースグループのメモリクォータの瞬時値。
  - `starrocks_be_resource_group_mem_allocated_bytes`: リソースグループのメモリ使用量の瞬時値。

## 次に行うこと

リソースグループを構成した後、メモリリソースとクエリを管理できます。詳細については、次のトピックを参照してください：

- [Memory management](../administration/Memory_management.md)

- [Query management](../administration/Query_management.md)