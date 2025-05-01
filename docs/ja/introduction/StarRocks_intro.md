---
displayed_sidebar: docs
---

# StarRocks

StarRocks は次世代の高性能分析データウェアハウスであり、リアルタイム、多次元、高度な同時実行データ分析を可能にします。StarRocks は MPP アーキテクチャを持ち、完全にベクトル化された実行エンジン、リアルタイム更新をサポートする列指向（カラムナ）ストレージエンジンを備え、完全にカスタマイズされたコストベースオプティマイザ（CBO）、インテリジェントなマテリアライズドビューなどの豊富な機能を備えています。StarRocks は、さまざまなデータソースからのリアルタイムおよびバッチデータ取り込みをサポートします。また、データレイクに保存されたデータをゼロデータ移行で直接分析することも可能です。

StarRocks は MySQL プロトコルと互換性があり、MySQL クライアントや一般的な BI ツールを使用して簡単に接続できます。StarRocks は非常にスケーラブルで、可用性が高く、メンテナンスが容易です。リアルタイム分析、アドホッククエリ、データレイク分析など、さまざまな OLAP シナリオで広く採用されています。

[StarRocks](https://github.com/StarRocks/starrocks/tree/main) は Apache 2.0 ライセンスの下で提供されており、StarRocks GitHub リポジトリで利用可能です（[StarRocks ライセンス](https://github.com/StarRocks/starrocks/blob/main/LICENSE.txt) を参照）。StarRocks は (i) サードパーティのソフトウェアライブラリからリンクまたは関数を呼び出し、そのライセンスはフォルダ [licenses-binary](https://github.com/StarRocks/starrocks/tree/main/licenses-binary) にあります。また、(ii) サードパーティのソフトウェアコードを組み込み、そのライセンスはフォルダ [licenses](https://github.com/StarRocks/starrocks/tree/main/licenses) にあります。

一般的な質問をするには、[ウェブフォーラム](https://forum.starrocks.io/) に参加してください。チャットには [Slack チャンネル](https://try.starrocks.com/join-starrocks-on-slack) に参加してください。コミュニティニュースについては、[StarRocks.io ブログ](https://www.starrocks.io/blog) をご覧ください。また、新機能、イベント、共有に関する最新情報を入手するには、[LinkedIn](https://www.linkedin.com/company/starrocks) をフォローしてください。

---

## 人気のトピック

### はじめに

[OLAP、機能、アーキテクチャ](../introduction/introduction.mdx)

### クイックスタート

[すぐに始める。](../quick_start/quick_start.mdx)

### データロード

[クリーン、変換、ロード](../loading/Loading_intro.md)

### テーブル設計

[テーブル、インデックス、アクセラレーション](../table_design/StarRocks_table_design.md)

### データレイク

[Iceberg、Hive、Delta Lake、…](../data_source/data_lakes.mdx)

### 半構造化データの操作

[JSON、map、struct、array](../sql-reference/sql-statements/data-types/JSON.md)

### 統合

[BI ツール、IDEs、クラウド認証、…](../integrations/integrations.mdx)

### 管理

[スケール、バックアップ、ロールと特権、…](../administration/administration.mdx)

### リファレンス

[SQL、関数、エラーコード、…](../reference/reference.mdx)

### よくある質問

[よくある質問。](../faq/faq.mdx)

### ベンチマーク

[DB パフォーマンス比較ベンチマーク。](../benchmarking/benchmarking.mdx)