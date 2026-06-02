---
displayed_sidebar: docs
description: OlapTable のアクティブなブックマーク（インベントリ、キャプチャされたパーティションバージョン、現在のホルダー）を表示します。
---

# table_bookmark_summary / _partitions / _references

`information_schema.table_bookmark_summary`、`table_bookmark_partitions`、`table_bookmark_references` は、StarRocks クラスター上のアクティブなブックマークの状態を提供します。ブックマークは、OlapTable のパーティション状態のイミュータブルな記録であり、ある時点で取得され、vacuum とタイムスタンプベースのルックアップに備えてパーティションバージョンを固定します。

3 つのテーブルは `(DB_ID, TABLE_ID, BOOKMARK_ID)` で結合します。

## table_bookmark_summary

**ブックマークインベントリ。** アクティブなブックマークごとに 1 行を返し、作成時刻、パーティション数 / 参照数、および概要集計（直近に変化した 3 つのパーティション、最も古い保持者と最も新しい保持者）を含みます。テーブルやクラスターにどのブックマークが存在するかを知りたいときに最初に問い合わせるテーブルです。

| カラム | 型 | 説明 |
|--------|------|-------------|
| DB_ID | BIGINT | 内部データベース ID。 |
| TABLE_ID | BIGINT | 内部テーブル ID。 |
| BOOKMARK_ID | BIGINT | ブックマーク ID（テーブル内で一意）。 |
| CREATE_TIME | DATETIME | ブックマークが取得された時刻。 |
| LOGICAL_PARTITION_COUNT | BIGINT | キャプチャされた論理パーティション数。 |
| PHYSICAL_PARTITION_COUNT | BIGINT | キャプチャされた物理パーティション数。 |
| REFERENCE_COUNT | BIGINT | このブックマークを参照している保持者の現在数。 |
| LATEST_CHANGED_PHYSICAL_PARTITIONS | ARRAY<STRUCT<id BIGINT, version BIGINT, time DATETIME>> | `visible_version_time` が最も新しい物理パーティションを最大 3 件、時刻の降順で返します。同値の場合は `physical_partition_id` が大きい方を優先します。ブックマークがパーティションをキャプチャしていない場合は空配列。パーティション数が 3 未満の場合は要素数も少なくなります。 |
| OLDEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME> | 現在の取得時刻が最も古い保持者。同値の場合は保持者 ID が辞書順で最小の方を優先します。 |
| NEWEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME> | 現在の取得時刻が最も新しい保持者。同値の場合の優先ルールは同じ。 |

## table_bookmark_partitions

**パーティション粒度の詳細。** (bookmark, physical_partition) の組み合わせごとに 1 行を返し、ブックマークがどのパーティションバージョンを固定しているかを正確に示します。vacuum をデバッグする際にこのテーブルを問い合わせ、`physical_partition_id` で `partitions_meta` と結合することで、現在のテーブルより古いバージョンを保持しているブックマークを特定できます。

| カラム | 型 | 説明 |
|--------|------|-------------|
| DB_ID, TABLE_ID, BOOKMARK_ID | (summary と同じ) | 結合キー。 |
| LOGICAL_PARTITION_ID | BIGINT | 論理パーティション ID。 |
| PHYSICAL_PARTITION_ID | BIGINT | 物理パーティション ID。 |
| VISIBLE_VERSION | BIGINT | このブックマークでキャプチャされたパーティションの可視バージョン。 |
| VISIBLE_VERSION_TIME | DATETIME | その可視バージョンが可視になった実時刻。 |
| BASE_MATERIALIZED_INDEX_META_ID | BIGINT | ブックマーク取得時点のベースマテリアライズドインデックスメタ ID。スキーマ変更 / リシャードで変化します。 |
| BASE_MATERIALIZED_INDEX_ID | BIGINT | ブックマーク取得時点のベースマテリアライズドインデックス ID。 |

## table_bookmark_references

**保持者粒度の詳細。** (bookmark, holder) の組み合わせごとに 1 行を返し、現在誰がブックマークを生存させているか、いつ取得したかを示します。特定のマテリアライズドビューやその他の保持者が保持しているブックマークを調べる際に問い合わせるテーブルです。

| カラム | 型 | 説明 |
|--------|------|-------------|
| DB_ID, TABLE_ID, BOOKMARK_ID | (summary と同じ) | 結合キー。 |
| HOLDER_ID | VARCHAR | 保持者の識別子。マテリアライズドビューは `mv:<dbId>-<mvId>` の形式でエンコードされます。 |
| CREATE_TIME | DATETIME | この保持者がブックマークを取得した時刻。 |

## クエリパターン

### インベントリ: テーブルの全ブックマークを列挙する

```sql
SELECT * FROM information_schema.table_bookmark_summary
WHERE table_id = <table_id>;
```

### Vacuum デバッグ: 最も古いブックマークが固定しているパーティションを調べる

```sql
SELECT p.physical_partition_id, p.visible_version
FROM information_schema.table_bookmark_partitions p
JOIN information_schema.table_bookmark_summary s
  USING (db_id, table_id, bookmark_id)
WHERE s.table_id = <table_id>
ORDER BY s.create_time ASC
LIMIT 100;
```

### MV 保持者ルックアップ: MV が保持しているブックマークを検索する

```sql
SELECT * FROM information_schema.table_bookmark_references
WHERE holder_id = 'mv:1001-2003';
```

## 注意事項

- 行は権限でフィルタされます。ユーザーは自身が `SELECT` 可能なテーブル上のブックマークのみを参照できます。
