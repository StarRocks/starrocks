---
displayed_sidebar: docs
---

# StarRocks バージョン 1.19

## 1.19.0

リリース日: 2021年10月22日

### 新機能

* グローバルランタイムフィルタを実装し、shuffle join に対するランタイムフィルタを有効にできます。
* CBO プランナーがデフォルトで有効化され、colocated join、バケットシャッフル、統計情報の推定などが改善されました。
* [実験的機能] 主キーテーブルリリース: リアルタイム/頻繁な更新機能をより良くサポートするために、StarRocks に新しいテーブルタイプ: 主キーテーブルが追加されました。主キーテーブルは Stream Load、Broker Load、Routine Load をサポートし、Flink-cdc に基づく MySQL データの秒単位の同期ツールも提供します。
* [実験的機能] 外部テーブルの書き込み機能をサポート。読み書き分離の要件を解決し、より良いリソース分離を提供するために、外部テーブルを介して別の StarRocks クラスターテーブルにデータを書き込むことをサポートします。

### 改善

#### StarRocks

* パフォーマンスの最適化。
  * count distinct int ステートメント
  * group by int ステートメント
  * or ステートメント
* ディスクバランスアルゴリズムを最適化。単一のマシンにディスクを追加した後、自動的にデータがバランスされます。
* 部分的なカラムエクスポートをサポート。
* show processlist を最適化して特定の SQL を表示。
* SET_VAR で複数の変数設定をサポート。
* エラーレポート情報を改善、table_sink、routine load、マテリアライズドビューの作成などを含む。

#### StarRocks-DataX コネクタ

* StarRocks-DataX Writer のフラッシュ間隔設定をサポート。

### バグ修正

* データリカバリ操作が完了した後、動的パーティションテーブルが自動的に作成されない問題を修正。 [# 337](https://github.com/StarRocks/starrocks/issues/337)
* CBO が開かれた後に row_number 関数でエラーが報告される問題を修正。
* 統計情報の収集により FE がスタックする問題を修正。
* set_var がセッションには有効だがステートメントには有効でない問題を修正。
* Hive パーティション外部テーブルで select count(*) が異常を返す問題を修正。

## 1.19.1

リリース日: 2021年11月2日

### 改善

* `show frontends` のパフォーマンスを最適化。[# 507](https://github.com/StarRocks/starrocks/pull/507) [# 984](https://github.com/StarRocks/starrocks/pull/984)
* スロークエリの監視を追加。[# 502](https://github.com/StarRocks/starrocks/pull/502) [# 891](https://github.com/StarRocks/starrocks/pull/891)
* Hive 外部メタデータの取得を最適化し、並行取得を実現。[# 425](https://github.com/StarRocks/starrocks/pull/425) [# 451](https://github.com/StarRocks/starrocks/pull/451)

### バグ修正

* Thrift プロトコルの互換性の問題を修正し、Hive 外部テーブルが Kerberos と接続できるように。[# 184](https://github.com/StarRocks/starrocks/pull/184) [# 947](https://github.com/StarRocks/starrocks/pull/947) [# 995](https://github.com/StarRocks/starrocks/pull/995) [# 999](https://github.com/StarRocks/starrocks/pull/999)
* ビュー作成におけるいくつかのバグを修正。[# 972](https://github.com/StarRocks/starrocks/pull/972) [# 987](https://github.com/StarRocks/starrocks/pull/987)[# 1001](https://github.com/StarRocks/starrocks/pull/1001)
* FE がグレースケールでアップグレードできない問題を修正。[# 485](https://github.com/StarRocks/starrocks/pull/485) [# 890](https://github.com/StarRocks/starrocks/pull/890)

## 1.19.2

リリース日: 2021年11月20日

### 改善

* バケットシャッフルジョインが右ジョインとフルアウタージョインをサポート [# 1209](https://github.com/StarRocks/starrocks/pull/1209)  [# 31234](https://github.com/StarRocks/starrocks/pull/1234)

### バグ修正

* リピートノードが述語プッシュダウンを行えない問題を修正[# 1410](https://github.com/StarRocks/starrocks/pull/1410) [# 1417](https://github.com/StarRocks/starrocks/pull/1417)
* クラスターがリーダーノードを変更する際に routine load がデータを失う可能性がある問題を修正。[# 1074](https://github.com/StarRocks/starrocks/pull/1074) [# 1272](https://github.com/StarRocks/starrocks/pull/1272)
* ビュー作成が union をサポートできない問題を修正 [# 1083](https://github.com/StarRocks/starrocks/pull/1083)
* Hive 外部テーブルのいくつかの安定性の問題を修正[# 1408](https://github.com/StarRocks/starrocks/pull/1408)
* group by ビューに関する問題を修正[# 1231](https://github.com/StarRocks/starrocks/pull/1231)

## 1.19.3

リリース日: 2021年11月30日

### 改善

* jprotobuf バージョンをアップグレードしてセキュリティを向上 [# 1506](https://github.com/StarRocks/starrocks/issues/1506)

### バグ修正

* group by 結果の正確性に関するいくつかの問題を修正
* grouping sets に関するいくつかの問題を修正[# 1395](https://github.com/StarRocks/starrocks/issues/1395) [# 1119](https://github.com/StarRocks/starrocks/pull/1119)
* date_format のいくつかの指標に関する問題を修正
* 集約ストリーミングの境界条件に関する問題を修正[# 1584](https://github.com/StarRocks/starrocks/pull/1584)
* 詳細については[リンク](https://github.com/StarRocks/starrocks/compare/1.19.2...1.19.3)を参照してください

## 1.19.4

リリース日: 2021年12月9日

### 改善

* cast(varchar as bitmap) をサポート [# 1941](https://github.com/StarRocks/starrocks/pull/1941)
* hive 外部テーブルのアクセスポリシーを更新 [# 1394](https://github.com/StarRocks/starrocks/pull/1394) [# 1807](https://github.com/StarRocks/starrocks/pull/1807)

### バグ修正

* predicate Cross Join による誤ったクエリ結果のバグを修正 [# 1918](https://github.com/StarRocks/starrocks/pull/1918)
* decimal 型と time 型の変換に関するバグを修正 [# 1709](https://github.com/StarRocks/starrocks/pull/1709) [# 1738](https://github.com/StarRocks/starrocks/pull/1738)
* colocate join/replicate join 選択エラーのバグを修正 [# 1727](https://github.com/StarRocks/starrocks/pull/1727)
* いくつかのプランコスト計算の問題を修正

## 1.19.5

リリース日: 2021年12月20日

### 改善

* shuffle join を最適化する計画 [# 2184](https://github.com/StarRocks/starrocks/pull/2184)
* 複数の大きなファイルのインポートを最適化 [# 2067](https://github.com/StarRocks/starrocks/pull/2067)

### バグ修正

* Log4j2 を 2.17.0 にアップグレードし、セキュリティ脆弱性を修正[# 2284](https://github.com/StarRocks/starrocks/pull/2284)[# 2290](https://github.com/StarRocks/starrocks/pull/2290)
* Hive 外部テーブルの空のパーティションの問題を修正[# 707](https://github.com/StarRocks/starrocks/pull/707)[# 2082](https://github.com/StarRocks/starrocks/pull/2082)

## 1.19.7

リリース日: 2022年3月18日

### バグ修正

以下のバグが修正されました:

* dataformat が異なるバージョンで異なる結果を生成する問題。[#4165](https://github.com/StarRocks/starrocks/pull/4165)
* データロード中に Parquet ファイルが誤って削除されることで BE ノードが失敗する可能性がある問題。[#3521](https://github.com/StarRocks/starrocks/pull/3521)