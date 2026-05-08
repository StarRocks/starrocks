---
displayed_sidebar: docs
---

# get_query_dump_from_query_id

`get_query_dump_from_query_id(query_id)`
`get_query_dump_from_query_id(query_id, enable_mock)`

`query_id` を指定して、過去に実行されたクエリのダンプを返します。ダンプには
そのクエリのプランニングを再現するために必要なテーブルスキーマ、統計情報、
セッション変数などのコンテキストが含まれます。形式は
[`get_query_dump`](./get_query_dump.md) や `/api/query_dump` HTTP エンドポイント
の出力と同一の JSON です。

本関数はデバッグおよび事後分析向けです。実行は次のように行います：現在の FE
上のクエリ詳細キュー (query detail queue) を `query_id` で検索し、元の SQL と
当時の catalog / database を取り出して、ダンパーで再生成します。

## 引数

`query_id`：StarRocks 標準の UUID 形式
(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) のクエリ識別子。型：VARCHAR。

`enable_mock`：（任意）`TRUE` のとき、実際のテーブル名 / カラム名を mock 値に
置き換えてダンプを生成します（スキーマを露出せずに dump を共有したい場合に
便利）。デフォルトは `FALSE`。型：BOOLEAN。

## 戻り値

VARCHAR。JSON でエンコードされたクエリダンプ。クエリが見つからない場合、
記録された SQL が脱感作されている場合、または呼び出し元が当該クエリを参照する
権限を持たない場合はエラーを送出します。

## 動作要件

データソースが query detail キューであるため、同キューの制約がそのまま本関数
にも適用されます。本関数は実行の冒頭で 2 つの FE 設定スイッチを前段検証し、
いずれかが想定外の値である場合、即座に明示的なエラーを返します：

- `enable_collect_query_detail_info` は `true` でなければなりません（デフォルト
  は `false`）。`false` の場合、誤解を招く "not found" の代わりに
  `query detail collection is disabled. Set FE config
  enable_collect_query_detail_info=true ...` というエラーで即時に拒否します。
- `enable_sql_desensitize_in_log` は `false` でなければなりません。`true` のとき、
  記録された SQL は digest 形式に書き換えられ、ダンプの再生成には使用できません。
  本関数は `SQL desensitization is enabled ...` で前段拒否します。

前段検証を通過した後でも、次の実行時制約が適用されます：

- 対象クエリは本関数を呼び出す FE と同じ FE で実行されている必要があります。
  detail は FE プロセス内のメモリ状態であり、FE 間で同期されません。
- detail 行は `query_detail_cache_time_nanosecond`（デフォルト 30 秒）の
  キャッシュウィンドウ内に存在する必要があります。期限切れの行はバックグラウンド
  処理で除去されます。
- 多層防御として、`enable_sql_desensitize_in_log` が現在 `false` であっても、
  detail 行が脱感作有効時に記録された場合（プレースホルダが格納されている場合）、
  本関数は `original sql not retained` でエラーを返します。

## 権限

呼び出し元の完全なアカウント識別子（`user`@`host`、例：`'alice'@'10.0.0.1'`）が
クエリの元実行者と完全に一致するか、システムレベルの `OPERATE` 権限を保持して
いる必要があります。ユーザー名のみが一致して host が異なる 2 つのアカウントは、
互いのクエリを参照できません。

## 例

```sql
-- 自分が実行したクエリを参照する
-- （前提：当該 FE で enable_collect_query_detail_info = true）
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789')\G

-- 同上、mock を有効にしてスキーマを露出せずに dump を取得
mysql> SELECT get_query_dump_from_query_id('a1b2c3d4-e5f6-7890-abcd-ef0123456789', TRUE)\G

-- キャッシュウィンドウ外 / ヒットしない場合の典型的なエラー
mysql> SELECT get_query_dump_from_query_id('00000000-0000-0000-0000-000000000000');
ERROR 1064 (HY000): Getting analyzing error. Detail message: Invalid parameter
get_query_dump_from_query_id: query_id not found in query detail queue: ...
```

## 関連項目

- [`get_query_dump`](./get_query_dump.md) — 呼び出し元が直接渡した SQL 文字列
  に対して dump を生成する関数。query detail キューを経由しません。
