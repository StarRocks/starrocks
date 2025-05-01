---
displayed_sidebar: docs
---

# おおよその重複排除にHLLを使用する

## 背景

実際のシナリオでは、データ量が増加するにつれて、データの重複排除の圧力が増します。データのサイズがあるレベルに達すると、正確な重複排除のコストは比較的高くなります。この場合、ユーザーは通常、計算の圧力を軽減するためにおおよそのアルゴリズムを使用します。このセクションで紹介する HyperLogLog (HLL) は、優れた空間計算量 O(mloglogn) と時間計算量 O(n) を持つおおよその重複排除アルゴリズムです。さらに、計算結果の誤差率は、データセットのサイズと使用されるハッシュ関数に応じて約1%-10%に制御できます。

## HyperLogLogとは

HyperLogLog は、非常に少ないストレージスペースを消費するおおよその重複排除アルゴリズムです。**HLL型**は、HyperLogLogアルゴリズムを実装するために使用されます。これは、HyperLogLog計算の中間結果を保持し、データテーブルのインジケーター列タイプとしてのみ使用できます。

HLLアルゴリズムは多くの数学的知識を含むため、実際の例を使用して説明します。コインが表になるまで独立した反復を行うランダム化実験Aを設計し、最初の表が出るまでのコイン投げの回数をランダム変数Xとして記録します。すると：

* X=1, P(X=1)=1/2
* X=2, P(X=2)=1/4
* ...
* X=n, P(X=n)=(1/2)<sup>n</sup>

テストAを使用してランダム化テストBを構築します。これは、テストAをN回独立して繰り返し、N個の独立した同一分布のランダム変数X<sub>1</sub>, X<sub>2</sub>, X<sub>3</sub>, ..., X<sub>N</sub>を生成します。ランダム変数の最大値をX<sub>max</sub>とします。大きな尤度推定を利用して、Nの推定値は2<sup>X<sub>max</sub></sup>です。
<br/>

次に、与えられたデータセットに対してハッシュ関数を使用して上記の実験をシミュレートします：

* テストA：データセット要素のハッシュ値を計算し、ハッシュ値を2進数表現に変換します。2進数の最下位ビットから始めて、ビット=1の出現を記録します。
* テストB：テストBのデータセット要素に対してテストAプロセスを繰り返します。各テストで最初のビット1の出現の最大位置「m」を更新します。
* データセット内の非重複要素の数をm<sup>2</sup>として推定します。

実際には、HLLアルゴリズムは要素をK=2<sup>k</sup>バケットに分割し、要素ハッシュの下位kビットに基づいています。k+1ビット目からの最初のビット1の出現の最大値をm<sub>1</sub>, m<sub>2</sub>,..., m<sub>k</sub>としてカウントし、バケット内の非重複要素の数を2<sup>m<sub>1</sub></sup>, 2<sup>m<sub>2</sub></sup>,..., 2<sup>m<sub>k</sub></sup>として推定します。データセット内の非重複要素の数は、バケットの数とバケット内の非重複要素の数を掛け合わせた合計平均です：N = K(K/(2<sup>\-m<sub>1</sub></sup>+2<sup>\-m<sub>2</sub></sup>,..., 2<sup>\-m<sub>K</sub></sup>))。
<br/>

HLLは、推定結果に補正係数を掛けて結果をより正確にします。

StarRocks SQLステートメントでHLL重複排除アルゴリズムを実装する方法については、記事 [https://gist.github.com/avibryant/8275649](https://gist.github.com/avibryant/8275649) を参照してください：

~~~sql
SELECT floor((0.721 * 1024 * 1024) / (sum(pow(2, m * -1)) + 1024 - count(*))) AS estimate
FROM(select(murmur_hash3_32(c2) & 1023) AS bucket,
     max((31 - CAST(log2(murmur_hash3_32(c2) & 2147483647) AS INT))) AS m
     FROM db0.table0
     GROUP BY bucket) bucket_values
~~~

このアルゴリズムは、db0.table0のcol2を重複排除します。

* ハッシュ関数 `murmur_hash3_32` を使用して、col2のハッシュ値を32ビット符号付き整数として計算します。
* 1024バケットを使用し、補正係数は0.721で、ハッシュ値の下位10ビットをバケットの添字として使用します。
* ハッシュ値の符号ビットを無視し、次の最高ビットから下位ビットまでの最初のビット1の出現位置を決定します。
* 計算されたハッシュ値をバケットでグループ化し、`MAX`集約を使用してバケット内の最初のビット1の出現位置の最大値を見つけます。
* 集約結果をサブクエリとして使用し、すべてのバケット推定値の合計平均にバケット数と補正係数を掛けます。
* 空のバケット数は1であることに注意してください。

データ量が多い場合、このアルゴリズムの誤差率は非常に低いです。

これがHLLアルゴリズムの核心です。興味がある方は [HyperLogLog paper](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) を参照してください。

### HyperLogLogの使用方法

1. HyperLogLog重複排除を使用するには、テーブル作成ステートメントでターゲットインジケーター列タイプを `HLL` に設定し、集約関数を `HLL_UNION` に設定する必要があります。
2. 現在、集約モデルのみがインジケーター列タイプとしてHLLをサポートしています。
3. HLL型の列で `count distinct` を使用する場合、StarRocksは自動的に `HLL_UNION_AGG` 計算に変換します。

#### 例

まず、**HLL** 列を持つテーブルを作成します。ここで、uvは集約列で、列タイプは `HLL` で、集約関数は [HLL_UNION](../sql-reference/sql-functions/aggregate-functions/hll_union.md) です。

~~~sql
CREATE TABLE test(
        dt DATE,
        id INT,
        uv HLL HLL_UNION
)
DISTRIBUTED BY HASH(ID) BUCKETS 32;
~~~

> * 注意: データ量が多い場合、高頻度のHLLクエリに対応するロールアップテーブルを作成する方が良いです。

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) を使用してデータをロードします：

~~~bash
curl --location-trusted -u <username>:<password> -H "label:label_1600997542287" \
    -H "column_separator:," \
    -H "columns:dt,id,user_id, uv=hll_hash(user_id)" -T /root/test.csv http://starrocks_be0:8040/api/db0/test/_stream_load
{
    "TxnId": 2504748,
    "Label": "label_1600997542287",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 5,
    "NumberLoadedRows": 5,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 120,
    "LoadTimeMs": 46,
    "BeginTxnTimeMs": 0,
    "StreamLoadPutTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 29,
    "CommitAndPublishTimeMs": 14
}
~~~

Broker Load モード：

~~~sql
LOAD LABEL test_db.label
 (
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file")
    INTO TABLE `test`
    COLUMNS TERMINATED BY ","
    (dt, id, user_id)
    SET (
      uv = HLL_HASH(user_id)
    )
 );
~~~

データのクエリ

* HLL列はその元の値を直接クエリすることはできません。関数 [HLL_UNION_AGG](../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md) を使用してクエリします。
* 合計uvを見つけるには、

`SELECT HLL_UNION_AGG(uv) FROM test;`

このステートメントは次のものと同等です

`SELECT COUNT(DISTINCT uv) FROM test;`

* 毎日のuvをクエリするには

`SELECT COUNT(DISTINCT uv) FROM test GROUP BY ID;`

### 注意事項

BitmapとHLLのどちらを選ぶべきか？データセットの基数が数百万または数千万で、数十台のマシンがある場合は、`count distinct` を使用します。基数が数億で正確な重複排除が必要な場合は `Bitmap` を使用します。おおよその重複排除が許容される場合は、`HLL` タイプを使用します。

BitmapはTINYINT、SMALLINT、INT、およびBIGINTのみをサポートします。LARGEINTはサポートされていないことに注意してください。重複排除する他のタイプのデータセットには、元のタイプを整数型にマッピングする辞書を構築する必要があります。辞書の構築は複雑で、データ量、更新頻度、クエリ効率、ストレージ、その他の問題とのトレードオフが必要です。HLLは辞書を必要としませんが、対応するデータタイプがハッシュ関数をサポートする必要があります。HLLを内部的にサポートしていない分析システムでも、ハッシュ関数とSQLを使用してHLL重複排除を実装することが可能です。

一般的な列には、ユーザーはおおよその重複排除のためにNDV関数を使用できます。この関数は、COUNT(DISTINCT col) 結果の近似集約を返し、基礎となる実装はデータストレージタイプをHyperLogLogタイプに変換して計算します。NDV関数は計算時に多くのリソースを消費するため、高い同時実行性のシナリオには適していません。

ユーザー行動分析を行いたい場合は、IntersectCountまたはカスタムUDAFを検討することができます。