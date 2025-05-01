---
displayed_sidebar: docs
---

# パフォーマンス最適化

## テーブルタイプの選択

StarRocks は 4 つのテーブルタイプをサポートしています: 重複キーテーブル、集計テーブル、ユニークキーテーブル、主キーテーブル。すべてのテーブルは KEY によってソートされます。

- `AGGREGATE KEY`: 同じ AGGREGATE KEY を持つレコードが StarRocks にロードされると、古いレコードと新しいレコードが集計されます。現在、集計テーブルは以下の集計関数をサポートしています: SUM、MIN、MAX、REPLACE。集計テーブルはデータを事前に集計することで、ビジネスステートメントや多次元分析を容易にします。
- `DUPLICATE KEY`: 重複キーテーブルではソートキーのみを指定する必要があります。同じ DUPLICATE KEY を持つレコードは同時に存在します。事前にデータを集計しない分析に適しています。
- `UNIQUE KEY`: 同じ UNIQUE KEY を持つレコードが StarRocks にロードされると、新しいレコードが古いレコードを上書きします。ユニークキーテーブルは、REPLACE 関数を持つ集計テーブルに似ています。どちらも定期的な更新を伴う分析に適しています。
- `PRIMARY KEY`: 主キーテーブルはレコードの一意性を保証し、リアルタイムの更新を可能にします。

~~~sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10;


CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid) BUCKETS 10;

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
PRIMARY KEY(orderid)
DISTRIBUTED BY HASH(orderid) BUCKETS 10;
~~~

## コロケートテーブル

クエリを高速化するために、同じ分散を持つテーブルは共通のバケット列を使用できます。この場合、`join` 操作中にデータをクラスター間で転送することなくローカルでジョインできます。

~~~sql
CREATE TABLE colocate_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

コロケートジョインとレプリカ管理の詳細については、 [Colocate join](../using_starrocks/Colocate_join.md) を参照してください。

## フラットテーブルとスタースキーマ

StarRocks はスタースキーマをサポートしており、フラットテーブルよりも柔軟にモデリングできます。モデリング中にフラットテーブルを置き換えるビューを作成し、複数のテーブルからデータをクエリしてクエリを高速化できます。

フラットテーブルには以下の欠点があります:

- フラットテーブルには通常大量のディメンションが含まれているため、ディメンションの更新コストが高くなります。ディメンションが更新されるたびに、テーブル全体を更新する必要があります。更新頻度が増すと状況は悪化します。
- フラットテーブルは追加の開発作業、ストレージスペース、データバックフィル操作を必要とするため、メンテナンスコストが高くなります。
- フラットテーブルには多くのフィールドがあり、集計テーブルにはさらに多くのキー フィールドが含まれる可能性があるため、データ取り込みコストが高くなります。データロード中に、より多くのフィールドをソートする必要があり、データロードが長引きます。

クエリの同時実行性や低レイテンシーに対する要求が高い場合は、フラットテーブルを使用することもできます。

## パーティションとバケット

StarRocks は 2 レベルのパーティショニングをサポートしています: 第一レベルは RANGE パーティション、第二レベルは HASH バケットです。

- RANGE パーティション: RANGE パーティションはデータを異なる間隔に分割するために使用されます（元のテーブルを複数のサブテーブルに分割することと理解できます）。ほとんどのユーザーは時間でパーティションを設定することを選択し、以下の利点があります:

  - ホットデータとコールドデータを区別しやすくなります
  - StarRocks の階層型ストレージ (SSD + SATA) を活用できます
  - パーティションごとにデータを削除するのが速くなります

- HASH バケット: ハッシュ値に従ってデータを異なるバケットに分割します。

  - データの偏りを避けるために、識別度の高い列をバケット化に使用することをお勧めします。
  - データの回復を容易にするために、各バケット内の圧縮データのサイズを 100 MB から 1 GB の間に保つことをお勧めします。テーブルを作成するかパーティションを追加する際に、適切なバケット数を設定することをお勧めします。
  - ランダムバケット法は推奨されません。テーブルを作成する際には、HASH バケット化列を明示的に指定する必要があります。

## スパースインデックスとブルームフィルターインデックス

StarRocks はデータを順序付けて保存し、1024 行の粒度でスパースインデックスを構築します。

StarRocks はスキーマ内の固定長プレフィックス（現在 36 バイト）をスパースインデックスとして選択します。

テーブルを作成する際には、一般的なフィルターフィールドをスキーマ宣言の先頭に配置することをお勧めします。識別度が最も高く、クエリ頻度が高いフィールドを最初に配置する必要があります。

VARCHAR フィールドはスパースインデックスの最後に配置する必要があります。インデックスは VARCHAR フィールドから切り捨てられるためです。VARCHAR フィールドが最初に現れると、インデックスは 36 バイト未満になる可能性があります。

上記の `site_visit` テーブルを例として使用します。このテーブルには 4 つの列があります: `siteid, city, username, pv`。ソートキーには 3 つの列 `siteid，city，username` が含まれ、それぞれ 4、2、32 バイトを占めています。したがって、プレフィックスインデックス（スパースインデックス）は `siteid + city + username` の最初の 30 バイトにすることができます。

スパースインデックスに加えて、StarRocks はブルームフィルターインデックスも提供しており、識別度の高い列のフィルタリングに効果的です。VARCHAR フィールドを他のフィールドの前に配置したい場合は、ブルームフィルターインデックスを作成できます。

## インバーテッドインデックス

StarRocks はビットマップインデックス技術を採用しており、重複キーテーブルのすべての列と集計テーブルおよびユニークキーテーブルのキー列に適用できるインバーテッドインデックスをサポートしています。ビットマップインデックスは、性別、都市、州などの小さな値範囲を持つ列に適しています。範囲が拡大するにつれて、ビットマップインデックスも並行して拡大します。

## マテリアライズドビュー (ロールアップ)

ロールアップは、元のテーブル（ベーステーブル）のマテリアライズドインデックスです。ロールアップを作成する際には、ベーステーブルの一部の列のみをスキーマとして選択でき、スキーマ内のフィールドの順序はベーステーブルと異なる場合があります。以下はロールアップの使用例です:

- ベーステーブルのデータ集約が高くない場合、ベーステーブルには識別度の高いフィールドが含まれているため、一部の列を選択してロールアップを作成することを検討できます。上記の `site_visit` テーブルを例として使用します:

  ~~~sql
  site_visit(siteid, city, username, pv)
  ~~~

  `siteid` はデータ集約を悪化させる可能性があります。都市ごとに PV を頻繁に計算する必要がある場合は、`city` と `pv` のみを含むロールアップを作成できます。

  ~~~sql
  ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
  ~~~

- ベーステーブルのプレフィックスインデックスがヒットしない場合、ベーステーブルの構築方法がすべてのクエリパターンをカバーできないため、列の順序を調整するためにロールアップを作成することを検討できます。上記の `session_data` テーブルを例として使用します:

  ~~~sql
  session_data(visitorid, sessionid, visittime, city, province, ip, brower, url)
  ~~~

  `visitorid` に加えて `browser` と `province` による訪問を分析する必要がある場合は、別のロールアップを作成できます:

  ~~~sql
  ALTER TABLE session_data
  ADD ROLLUP rollup_brower(brower,province,ip,url)
  DUPLICATE KEY(brower,province);
  ~~~

## スキーマ変更

StarRocks では、スキーマを変更する方法が 3 つあります: ソートスキーマ変更、直接スキーマ変更、リンクスキーマ変更。

- ソートスキーマ変更: 列のソートを変更し、データを再配置します。たとえば、ソートスキーマで列を削除するとデータが再配置されます。

  `ALTER TABLE site_visit DROP COLUMN city;`

- 直接スキーマ変更: データを再配置するのではなく変換します。たとえば、列の型を変更したり、スパースインデックスに列を追加したりします。

  `ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

- リンクスキーマ変更: データを変換せずに変更を完了します。たとえば、列を追加します。

  `ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

  スキーマ変更を加速するために、テーブルを作成する際には適切なスキーマを選択することをお勧めします。