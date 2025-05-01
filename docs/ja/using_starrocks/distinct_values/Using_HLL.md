---
displayed_sidebar: docs
sidebar_position: 20
---

# 重複排除のための HLL の使用

## 背景

現実のシナリオでは、データ量が増えるにつれてデータの重複排除の圧力が増します。データサイズがあるレベルに達すると、正確な重複排除のコストが比較的高くなります。この場合、ユーザーは通常、計算の負荷を軽減するために近似アルゴリズムを使用します。このセクションで紹介する HyperLogLog (HLL) は、優れた空間計算量 O(mloglogn) と時間計算量 O(n) を持つ近似重複排除アルゴリズムです。さらに、計算結果の誤差率は、データセットのサイズや使用されるハッシュ関数に応じて約 1% ～ 10% に制御できます。

## HyperLogLog とは

HyperLogLog は、非常に少ないストレージスペースを消費する近似重複排除アルゴリズムです。**HLL タイプ**は、HyperLogLog アルゴリズムを実装するために使用されます。これは、HyperLogLog 計算の中間結果を保持し、データテーブルのインジケーター列タイプとしてのみ使用できます。

HLL アルゴリズムは多くの数学的知識を含むため、実用的な例を用いて説明します。コインを投げて最初に表が出るまで独立した反復を行うランダム化実験 A を設計し、最初に表が出るまでのコイン投げの回数をランダム変数 X として記録するとします。すると：

* X=1, P(X=1)=1/2
* X=2, P(X=2)=1/4
* ...
* X=n, P(X=n)=(1/2)<sup>n</sup>

テスト A を使用してランダム化テスト B を構築し、N 回の独立したテスト A の反復を行い、N 個の独立した同一分布のランダム変数 X<sub>1</sub>, X<sub>2</sub>, X<sub>3</sub>, ..., X<sub>N</sub> を生成します。ランダム変数の最大値を X<sub>max</sub> とします。大きな尤度推定を利用して、N の推定値は 2<sup>X<sub>max</sub></sup> です。

与えられたデータセットに対してハッシュ関数を使用して上記の実験をシミュレートします：

* テスト A: データセット要素のハッシュ値を計算し、ハッシュ値をバイナリ表現に変換します。バイナリの最下位ビットから始めて、ビット=1 の出現を記録します。
* テスト B: テスト B のデータセット要素に対してテスト A プロセスを繰り返します。各テストの最初のビット 1 の出現の最大位置 "m" を更新します。
* データセット内の重複しない要素の数を m<sup>2</sup> として推定します。

実際には、HLL アルゴリズムは要素のハッシュの下位 k ビットに基づいて要素を K=2<sup>k</sup> バケットに分割します。k+1 ビット目からの最初のビット 1 の出現の最大値を m<sub>1</sub>, m<sub>2</sub>,..., m<sub>k</sub> としてカウントし、バケット内の重複しない要素の数を 2<sup>m<sub>1</sub></sup>, 2<sup>m<sub>2</sub></sup>,..., 2<sup>m<sub>k</sub></sup> として推定します。データセット内の重複しない要素の数は、バケットの数とバケット内の重複しない要素の数を掛けた合計平均です：N = K(K/(2<sup>\-m<sub>1</sub></sup>+2<sup>\-m<sub>2</sub></sup>,..., 2<sup>\-m<sub>K</sub></sup>))。

HLL は推定結果に補正係数を掛けて結果をより正確にします。

StarRocks SQL ステートメントを使用して HLL 重複排除アルゴリズムを実装する方法については、次の記事を参照してください [https://gist.github.com/avibryant/8275649](https://gist.github.com/avibryant/8275649) 。

```sql
SELECT floor((0.721 * 1024 * 1024) / (sum(pow(2, m * -1)) + 1024 - count(*))) AS estimate
FROM(select(murmur_hash3_32(c2) & 1023) AS bucket,
     max((31 - CAST(log2(murmur_hash3_32(c2) & 2147483647) AS INT))) AS m
     FROM db0.table0
     GROUP BY bucket) bucket_values
```

このアルゴリズムは db0.table0 の col2 を重複排除します。

* ハッシュ関数 `murmur_hash3_32` を使用して col2 のハッシュ値を 32 ビット符号付き整数として計算します。
* 1024 バケットを使用し、補正係数は 0.721 で、ハッシュ値の下位 10 ビットをバケットの添字として使用します。
* ハッシュ値の符号ビットを無視し、次の最上位ビットから下位ビットまでを開始し、最初のビット 1 の出現位置を決定します。
* 計算されたハッシュ値をバケットでグループ化し、`MAX` 集約を使用してバケット内の最初のビット 1 の出現の最大位置を見つけます。
* 集約結果はサブクエリとして使用され、すべてのバケット推定の合計平均にバケットの数と補正係数を掛けます。
* 空のバケット数は 1 です。

このアルゴリズムは、データ量が多い場合に非常に低い誤差率を持ちます。

これが HLL アルゴリズムの核心です。興味がある方は [HyperLogLog 論文](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) を参照してください。

### HyperLogLog の使用方法

1. HyperLogLog 重複排除を使用するには、テーブル作成ステートメントでターゲットインジケーター列タイプを `HLL` に設定し、集約関数を `HLL_UNION` に設定する必要があります。
2. 現在、集約モデルのみがインジケーター列タイプとして HLL をサポートしています。
3. HLL タイプの列で `count distinct` を使用する場合、StarRocks は自動的に `HLL_UNION_AGG` 計算に変換します。

#### 例

まず、**HLL** 列を持つテーブルを作成します。ここで uv は集約列で、列タイプは `HLL` であり、集約関数は [HLL_UNION](../../sql-reference/sql-functions/aggregate-functions/hll_union.md) です。

```sql
CREATE TABLE test(
        dt DATE,
        id INT,
        uv HLL HLL_UNION
)
DISTRIBUTED BY HASH(ID);
```

> * 注: データ量が多い場合、高頻度の HLL クエリに対して対応するロールアップテーブルを作成する方が良いです。

[Stream Load](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を使用してデータをロードします：

```bash
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
```

Broker Load モード：

```sql
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
```

データのクエリ

* HLL 列はその元の値を直接クエリすることはできません。関数 [HLL_UNION_AGG](../../sql-reference/sql-functions/aggregate-functions/hll_union_agg.md) を使用してクエリします。
* 合計 uv を見つけるには、

`SELECT HLL_UNION_AGG(uv) FROM test;`

このステートメントは次のものと同等です

`SELECT COUNT(DISTINCT uv) FROM test;`

* 毎日の uv をクエリするには

`SELECT COUNT(DISTINCT uv) FROM test GROUP BY ID;`

### 注意事項

Bitmap と HLL のどちらを選ぶべきか？データセットの基数が数百万または数千万で、数十台のマシンがある場合は、`count distinct` を使用します。基数が数億で正確な重複排除が必要な場合は `Bitmap` を使用します。近似重複排除が許容される場合は、`HLL` タイプを使用します。

Bitmap は TINYINT、SMALLINT、INT、および BIGINT のみをサポートします。LARGEINT はサポートされていないことに注意してください。重複排除する他のタイプのデータセットには、元のタイプを整数タイプにマッピングする辞書を構築する必要があります。辞書の構築は複雑であり、データ量、更新頻度、クエリ効率、ストレージ、その他の問題とのトレードオフが必要です。HLL は辞書を必要としませんが、対応するデータタイプがハッシュ関数をサポートする必要があります。HLL を内部でサポートしていない分析システムでも、ハッシュ関数と SQL を使用して HLL 重複排除を実装することが可能です。

一般的な列に対しては、ユーザーは NDV 関数を使用して近似重複排除を行うことができます。この関数は COUNT(DISTINCT col) 結果の近似集約を返し、基礎となる実装はデータストレージタイプを HyperLogLog タイプに変換して計算します。NDV 関数は計算時に多くのリソースを消費するため、高い同時実行性のシナリオには適していません。

ユーザー行動分析を行いたい場合は、IntersectCount またはカスタム UDAF を検討することができます。