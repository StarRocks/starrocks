---
displayed_sidebar: docs
sidebar_position: 40
---

# Colocate Join

Shuffle Join と Broadcast Join では、ジョイン条件が満たされると、2 つのジョインテーブルのデータ行が単一のノードにマージされてジョインが完了します。これらの 2 つのジョイン方法のいずれも、ノード間のデータネットワーク伝送によって引き起こされる遅延やオーバーヘッドを回避することはできません。

コアアイデアは、同じ Colocation Group 内のテーブルに対して、バケッティングキー、コピーの数、コピーの配置を一貫して保つことです。ジョイン列がバケッティングキーである場合、計算ノードは他のノードからデータを取得することなくローカルジョインを行うだけで済みます。Colocate Join は等値ジョインをサポートします。

このドキュメントでは、Colocate Join の原理、実装、使用法、および考慮事項を紹介します。

## 用語

* **Colocation Group (CG)**: CG には 1 つ以上のテーブルが含まれます。CG 内のテーブルは同じバケッティングとレプリカ配置を持ち、Colocation Group Schema を使用して記述されます。
* **Colocation Group Schema (CGS)**: CGS には、CG のバケッティングキー、バケットの数、およびレプリカの数が含まれます。

## 原理

Colocate Join は、同じ CGS を持つ一連のテーブルで CG を形成し、これらのテーブルの対応するバケットコピーが同じ BE ノードセットに配置されることを保証します。CG 内のテーブルがバケット化された列でジョイン操作を行うとき、ローカルデータを直接ジョインでき、ノード間のデータ転送の時間を節約できます。

バケットシーケンスは `hash(key) mod buckets` によって取得されます。たとえば、テーブルが 8 バケットを持っている場合、\[0, 1, 2, 3, 4, 5, 6, 7\] の 8 バケットがあり、各バケットには 1 つ以上のサブテーブルがあり、サブテーブルの数はパーティションの数に依存します。マルチパーティションテーブルの場合、複数のタブレットがあります。

同じデータ分布を持つために、同じ CG 内のテーブルは次のことを遵守する必要があります。

1. 同じ CG 内のテーブルは、同一のバケッティングキー（タイプ、数、順序）と同じ数のバケットを持たなければなりません。これにより、複数のテーブルのデータスライスを一対一で分配および制御できます。バケッティングキーは、テーブル作成ステートメント `DISTRIBUTED BY HASH(col1, col2, ...)` で指定された列です。バケッティングキーは、データのどの列が異なるバケットシーケンスにハッシュされるかを決定します。同じ CG 内のテーブルでバケッティングキーの名前は異なる場合があります。作成ステートメントでバケッティング列が異なる場合がありますが、`DISTRIBUTED BY HASH(col1, col2, ...)` の対応するデータ型の順序は完全に同じである必要があります。
2. 同じ CG 内のテーブルは、同じ数のパーティションコピーを持たなければなりません。そうでない場合、タブレットコピーが同じ BE のパーティションに対応するコピーを持たないことがあるかもしれません。
3. 同じ CG 内のテーブルは、異なる数のパーティションと異なるパーティションキーを持つことができます。

テーブルを作成するとき、CG はテーブルプロパティ内の属性 `"colocate_with" = "group_name"` によって指定されます。CG が存在しない場合、それはテーブルが CG の最初のテーブルであり、親テーブルと呼ばれます。親テーブルのデータ分布（スプリットバケットキーのタイプ、数、順序、コピーの数、スプリットバケットの数）が CGS を決定します。CG が存在する場合、テーブルのデータ分布が CGS と一致しているかどうかを確認します。

同じ CG 内のテーブルのコピー配置は次の条件を満たします。

1. すべてのテーブルのバケットシーケンスと BE ノードのマッピングは、親テーブルと同じです。
2. 親テーブル内のすべてのパーティションのバケットシーケンスと BE ノードのマッピングは、最初のパーティションと同じです。
3. 親テーブルの最初のパーティションのバケットシーケンスと BE ノードのマッピングは、ネイティブのラウンドロビンアルゴリズムを使用して決定されます。

一貫したデータ分布とマッピングにより、バケッティングキーで取得された同じ値を持つデータ行が同じ BE に配置されることが保証されます。したがって、バケッティングキーを使用して列をジョインする場合、ローカルジョインのみが必要です。

## 使用法

### テーブル作成

テーブルを作成する際に、PROPERTIES で属性 `"colocate_with" = "group_name"` を指定して、そのテーブルが Colocate Join テーブルであり、指定された Colocation Group に属することを示すことができます。
> **注意**
>
> バージョン 2.5.4 以降、異なるデータベースからのテーブルで Colocate Join を実行できます。テーブルを作成する際に同じ `colocate_with` プロパティを指定するだけです。

例:

~~~SQL
CREATE TABLE tbl (k1 int, v1 int sum)
DISTRIBUTED BY HASH(k1)
BUCKETS 8
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

指定されたグループが存在しない場合、StarRocks は現在のテーブルのみを含むグループを自動的に作成します。グループが存在する場合、StarRocks は現在のテーブルが Colocation Group Schema を満たしているかどうかを確認します。満たしている場合、テーブルを作成し、グループに追加します。同時に、テーブルは既存のグループのデータ分布ルールに基づいてパーティションとタブレットを作成します。

Colocation Group はデータベースに属します。Colocation Group の名前はデータベース内で一意です。内部ストレージでは、Colocation Group の完全な名前は `dbId_groupName` ですが、`groupName` だけを認識します。
> **注意**
>
> 異なるデータベースからのテーブルを Colocate Join に関連付けるために同じ Colocation Group を指定する場合、Colocation Group はこれらのデータベースのそれぞれに存在します。異なるデータベースの Colocation Group を確認するには、`show proc "/colocation_group"` を実行できます。

### 削除

完全な削除は、リサイクルビンからの削除です。通常、`DROP TABLE` コマンドでテーブルを削除した後、デフォルトでは 1 日間リサイクルビンに残り、その後削除されます。グループ内の最後のテーブルが完全に削除されると、グループも自動的に削除されます。

### グループ情報の表示

次のコマンドを使用して、クラスター内に既に存在するグループ情報を表示できます。

~~~Plain Text
SHOW PROC '/colocation_group';

+-------------+--------------+--------------+------------+----------------+----------+----------+
| GroupId     | GroupName    | TableIds     | BucketsNum | ReplicationNum | DistCols | IsStable |
+-------------+--------------+--------------+------------+----------------+----------+----------+
| 10005.10008 | 10005_group1 | 10007, 10040 | 10         | 3              | int(11)  | true     |
+-------------+--------------+--------------+------------+----------------+----------+----------+
~~~

* **GroupId**: クラスター全体で一意のグループ識別子で、前半が db id、後半がグループ id です。
* **GroupName**: グループの完全な名前。
* **TabletIds**: グループ内のテーブルの id リスト。
* **BucketsNum**: バケットの数。
* **ReplicationNum**: レプリカの数。
* **DistCols**: 分布列、つまりバケッティング列のタイプ。
* **IsStable**: グループが安定しているかどうか（安定性の定義については、Colocation Replica Balancing and Repair のセクションを参照してください）。

次のコマンドを使用して、グループのデータ分布をさらに表示できます。

~~~Plain Text
SHOW PROC '/colocation_group/10005.10008';

+-------------+---------------------+
| BucketIndex | BackendIds          |
+-------------+---------------------+
| 0           | 10004, 10002, 10001 |
| 1           | 10003, 10002, 10004 |
| 2           | 10002, 10004, 10001 |
| 3           | 10003, 10002, 10004 |
| 4           | 10002, 10004, 10003 |
| 5           | 10003, 10002, 10001 |
| 6           | 10003, 10004, 10001 |
| 7           | 10003, 10004, 10002 |
+-------------+---------------------+
~~~

* **BucketIndex**: バケットのシーケンスの添字。
* **BackendIds**: バケッティングデータスライスが配置されている BE ノードの id。

> 注意: 上記のコマンドを使用するには、NODE 権限または `cluster_admin` ロールが必要です。通常のユーザーはアクセスできません。

### テーブルグループプロパティの変更

テーブルの Colocation Group プロパティを変更できます。例:

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "group2");
~~~

テーブルが以前にグループに割り当てられていない場合、コマンドはスキーマを確認し、テーブルをグループに追加します（グループが存在しない場合は最初に作成されます）。テーブルが以前に別のグループに割り当てられていた場合、コマンドはテーブルを元のグループから削除し、新しいグループに追加します（グループが存在しない場合は最初に作成されます）。

次のコマンドを使用して、テーブルの Colocation プロパティを削除することもできます。

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "");
~~~

### その他の関連操作

Colocation 属性を持つテーブルに `ADD PARTITION` を使用してパーティションを追加したり、コピーの数を変更したりする場合、StarRocks はその操作が Colocation Group Schema に違反するかどうかを確認し、違反する場合は拒否します。

## Colocation レプリカのバランシングと修復

Colocation テーブルのレプリカ分布は、グループスキーマで指定された分布ルールに従う必要があるため、通常のシャーディングとはレプリカの修復とバランシングの点で異なります。

グループ自体には `stable` プロパティがあります。`stable` が `true` の場合、グループ内のテーブルスライスに変更が加えられておらず、Colocation 機能が正常に動作していることを意味します。`stable` が `false` の場合、現在のグループ内の一部のテーブルスライスが修復または移行されており、影響を受けたテーブルの Colocate Join が通常のジョインに劣化することを意味します。

### レプリカ修復

レプリカは指定された BE ノードにのみ保存できます。StarRocks は、利用できない BE（例: ダウン、廃止）を置き換えるために最も負荷の少ない BE を探します。置き換え後、古い BE 上のすべてのバケッティングデータスライスが修復されます。移行中、グループは **Unstable** とマークされます。

### レプリカバランシング

StarRocks は、Colocation テーブルスライスをすべての BE ノードに均等に分配しようとします。通常のテーブルのバランシングはレプリカレベルで行われ、各レプリカが個別に負荷の低い BE ノードを見つけます。Colocation テーブルのバランシングはバケットレベルで行われ、バケット内のすべてのレプリカが一緒に移行されます。`BucketsSequnce` をすべての BE ノードに均等に分配する単純なバランシングアルゴリズムを使用し、レプリカの実際のサイズではなく、レプリカの数のみを考慮します。正確なアルゴリズムは `ColocateTableBalancer.java` のコードコメントに記載されています。

> 注意 1: 現在の Colocation レプリカのバランシングと修復アルゴリズムは、異種展開を持つ StarRocks クラスターではうまく機能しない可能性があります。いわゆる異種展開とは、BE ノードのディスク容量、ディスクの数、ディスクタイプ（SSD と HDD）が一貫していないことを意味します。異種展開の場合、小容量の BE ノードが大容量の BE ノードと同じ数のレプリカを保存することがあるかもしれません。
>
> 注意 2: グループが Unstable 状態にある場合、そのテーブルのジョインは通常のジョインに劣化し、クラスターのクエリパフォーマンスが大幅に低下する可能性があります。システムが自動的にバランスを取らないようにしたい場合は、FE 設定 `disable_colocate_balance` を設定して自動バランシングを無効にし、適切なタイミングで再度有効にします。（詳細については、Advanced Operations セクションを参照してください）

## クエリ

Colocation テーブルは通常のテーブルと同じ方法でクエリされます。Colocation テーブルがあるグループが Unstable 状態にある場合、それは自動的に通常のジョインに劣化します。以下の例で説明します。

テーブル 1:

~~~SQL
CREATE TABLE `tbl1` (
    `k1` date NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` int(11) SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
PARTITION BY RANGE(`k1`)
(
    PARTITION p1 VALUES LESS THAN ('2019-05-31'),
    PARTITION p2 VALUES LESS THAN ('2019-06-30')
)
DISTRIBUTED BY HASH(`k2`)  BUCKETS 6
PROPERTIES (
    "colocate_with" = "group1"
);
INSERT INTO tbl1
VALUES
    ("2015-09-12",1000,1),
    ("2015-09-13",2000,2);
~~~

テーブル 2:

~~~SQL
CREATE TABLE `tbl2` (
    `k1` datetime NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` double SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k2`)  BUCKETS 6
PROPERTIES (
    "colocate_with" = "group1"
);
INSERT INTO tbl2
VALUES
    ("2015-09-12 00:00:00",3000,3),
    ("2015-09-12 00:00:00",4000,4);
~~~

クエリプランの表示:

~~~Plain Text
EXPLAIN SELECT * FROM tbl1 INNER JOIN tbl2 ON (tbl1.k2 = tbl2.k2);
+-------------------------------------------------------------------------+
| Explain String                                                          |
+-------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                         |
|  OUTPUT EXPRS:1: k1 | 2: k2 | 3: v1 | 4: k1 | 5: k2 | 6: v1             |
|   PARTITION: UNPARTITIONED                                              |
|                                                                         |
|   RESULT SINK                                                           |
|                                                                         |
|   3:EXCHANGE                                                            |
|                                                                         |
| PLAN FRAGMENT 1                                                         |
|  OUTPUT EXPRS:                                                          |
|   PARTITION: RANDOM                                                     |
|                                                                         |
|   STREAM DATA SINK                                                      |
|     EXCHANGE ID: 03                                                     |
|     UNPARTITIONED                                                       |
|                                                                         |
|   2:HASH JOIN                                                           |
|   |  join op: INNER JOIN (COLOCATE)                                     |
|   |  colocate: true                                                     |
|   |  equal join conjunct: 5: k2 = 2: k2                                 |
|   |                                                                     |
|   |----1:OlapScanNode                                                   |
|   |       TABLE: tbl1                                                   |
|   |       PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join |
|   |       partitions=1/2                                                |
|   |       rollup: tbl1                                                  |
|   |       tabletRatio=6/6                                               |
|   |       tabletList=15344,15346,15348,15350,15352,15354                |
|   |       cardinality=1                                                 |
|   |       avgRowSize=3.0                                                |
|   |                                                                     |
|   0:OlapScanNode                                                        |
|      TABLE: tbl2                                                        |
|      PREAGGREGATION: OFF. Reason: None aggregate function               |
|      partitions=1/1                                                     |
|      rollup: tbl2                                                       |
|      tabletRatio=6/6                                                    |
|      tabletList=15373,15375,15377,15379,15381,15383                     |
|      cardinality=1                                                      |
|      avgRowSize=3.0                                                     |
+-------------------------------------------------------------------------+
40 rows in set (0.03 sec)
~~~

Colocate Join が有効になると、Hash Join ノードに `colocate: true` と表示されます。

有効でない場合、クエリプランは次のようになります。

~~~Plain Text
+----------------------------------------------------+
| Explain String                                     |
+----------------------------------------------------+
| PLAN FRAGMENT 0                                    |
|  OUTPUT EXPRS:`tbl1`.`k1` |                        |
|   PARTITION: RANDOM                                |
|                                                    |
|   RESULT SINK                                      |
|                                                    |
|   2:HASH JOIN                                      |
|   |  join op: INNER JOIN (BROADCAST)               |
|   |  hash predicates:                              |
|   |  colocate: false, reason: group is not stable  |
|   |    `tbl1`.`k2` = `tbl2`.`k2`                   |
|   |  tuple ids: 0 1                                |
|   |                                                |
|   |----3:EXCHANGE                                  |
|   |       tuple ids: 1                             |
|   |                                                |
|   0:OlapScanNode                                   |
|      TABLE: tbl1                                   |
|      PREAGGREGATION: OFF. Reason: No AggregateInfo |
|      partitions=0/2                                |
|      rollup: null                                  |
|      buckets=0/0                                   |
|      cardinality=-1                                |
|      avgRowSize=0.0                                |
|      numNodes=0                                    |
|      tuple ids: 0                                  |
|                                                    |
| PLAN FRAGMENT 1                                    |
|  OUTPUT EXPRS:                                     |
|   PARTITION: RANDOM                                |
|                                                    |
|   STREAM DATA SINK                                 |
|     EXCHANGE ID: 03                                |
|     UNPARTITIONED                                  |
|                                                    |
|   1:OlapScanNode                                   |
|      TABLE: tbl2                                   |
|      PREAGGREGATION: OFF. Reason: null             |
|      partitions=0/1                                |
|      rollup: null                                  |
|      buckets=0/0                                   |
|      cardinality=-1                                |
|      avgRowSize=0.0                                 |
|      numNodes=0                                    |
|      tuple ids: 1                                  |
+----------------------------------------------------+
~~~

HASH JOIN ノードは、対応する理由を示します: `colocate: false, reason: group is not stable`。同時に EXCHANGE ノードが生成されます。

## 高度な操作

### FE 設定項目

* **disable_colocate_relocate**

StarRocks の自動 Colocation レプリカ修復を無効にするかどうか。デフォルトは false で、オンになっています。このパラメータは、Colocation テーブルのレプリカ修復にのみ影響し、通常のテーブルには影響しません。

* **disable_colocate_balance**

StarRocks の自動 Colocation レプリカバランシングを無効にするかどうか。デフォルトは false で、オンになっています。このパラメータは、Colocation テーブルのレプリカバランシングにのみ影響し、通常のテーブルには影響しません。

* **disable_colocate_join**

    この変数を変更することで、セッション単位で Colocate Join を無効にできます。

* **disable_colocate_join**

    この変数を変更することで、Colocate Join 機能を無効にできます。

### HTTP Restful API

StarRocks は、Colocate Join に関連する Colocation Group の表示および変更に関するいくつかの HTTP Restful API を提供しています。

この API は FE 上で実装されており、ADMIN 権限を使用して `fe_host:fe_http_port` でアクセスできます。

1. クラスターのすべての Colocation 情報を表示

    ~~~bash
    curl --location-trusted -u<username>:<password> 'http://<fe_host>:<fe_http_port>/api/colocate'  
    ~~~

    ~~~JSON
    // Json 形式で内部の Colocation 情報を返します。
    {
        "colocate_meta": {
            "groupName2Id": {
                "g1": {
                    "dbId": 10005,
                    "grpId": 10008
                }
            },
            "group2Tables": {},
            "table2Group": {
                "10007": {
                    "dbId": 10005,
                    "grpId": 10008
                },
                "10040": {
                    "dbId": 10005,
                    "grpId": 10008
                }
            },
            "group2Schema": {
                "10005.10008": {
                    "groupId": {
                        "dbId": 10005,
                        "grpId": 10008
                    },
                    "distributionColTypes": [{
                        "type": "INT",
                        "len": -1,
                        "isAssignedStrLenInColDefinition": false,
                        "precision": 0,
                        "scale": 0
                    }],
                    "bucketsNum": 10,
                    "replicationNum": 2
                }
            },
            "group2BackendsPerBucketSeq": {
                "10005.10008": [
                    [10004, 10002],
                    [10003, 10002],
                    [10002, 10004],
                    [10003, 10002],
                    [10002, 10004],
                    [10003, 10002],
                    [10003, 10004],
                    [10003, 10004],
                    [10003, 10004],
                    [10002, 10004]
                ]
            },
            "unstableGroups": []
        },
        "status": "OK"
    }
    ~~~

2. グループを Stable または Unstable にマーク

    ~~~bash
    # Stable にマーク
    curl -XPOST --location-trusted -u<username>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_stable?db_id=<dbId>&group_id=<grpId>​'
    # Unstable にマーク
    curl -XPOST --location-trusted -u<username>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_unstable?db_id=<dbId>&group_id=<grpId>​'
    ~~~

    返された結果が `200` の場合、グループは正常に Stable または Unstable にマークされます。

3. グループのデータ分布を設定

    このインターフェースを使用して、グループの数分布を強制的に設定できます。

    `POST /api/colocate/bucketseq?db_id=10005&group_id= 10008`

    `Body:`

    `[[10004,10002],[10003,10002],[10002,10004],[10003,10002],[10002,10004],[10003,10002],[10003,10004],[10003,10004],[10003,10004],[10002,10004]]`

    `returns: 200`

    `Body` は、ネストされた配列として表される `BucketsSequence` であり、バケッティングスライスが配置されている BE の id です。

    > このコマンドを使用するには、FE 設定 `disable_colocate_relocate` と `disable_colocate_balance` を true に設定し、システムが自動的に Colocation レプリカの修復とバランシングを行わないようにする必要があるかもしれません。そうしないと、変更後にシステムによって自動的にリセットされる可能性があります。