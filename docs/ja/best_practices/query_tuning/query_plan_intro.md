---
displayed_sidebar: docs
sidebar_position: 10
---

# クエリチューニングの紹介

クエリチューニングは、StarRocks において高いパフォーマンスと信頼性を実現するために不可欠です。このディレクトリでは、実用的なガイド、参考資料、実行可能なレシピを集め、SQL の記述から実行詳細の解釈に至るまで、あらゆる段階でクエリパフォーマンスを分析、診断、最適化するのに役立ちます。

StarRocks での効果的なクエリチューニングは、通常、トップダウンのプロセスに従います。

1. **問題の特定**  
   - 遅いクエリ、高いリソース使用率、または予期しない結果を検出します。  
   - StarRocks では、組み込みの監視ツール、クエリ履歴、監査ログを活用して、問題のあるクエリや異常なパターンを迅速に特定します。  
   - 参照: ** [Query Tuning Recipes](./query_profile_tuning_recipes.md) ** で症状に基づく診断を行い、** [Query Profile Overview](./query_profile_overview.md) ** でクエリ履歴とプロファイルにアクセスします。

2. **実行情報の収集と分析**  
   - `EXPLAIN` または `EXPLAIN ANALYZE` を使用してクエリプランを取得します。  
   - Query Profile を有効にして、詳細な実行メトリクスを収集します。  
   - 参照: ** [Query Plan Overview](./query_planning.md) ** でクエリプランの理解を深め、** [Explain Analyze & Text-Based Profile Analysis](./query_profile_text_based_analysis.md) ** でステップバイステップの分析を行い、** [Query Profile Overview](./query_profile_overview.md) ** でプロファイルの有効化と解釈を行います。

3. **根本原因の特定**  
   - どのステージや Operator が最も時間やリソースを消費しているかを特定します。  
   - よくある問題をチェックします: 非最適なジョイン順序、インデックスの欠如、データ分布の問題、または非効率な SQL パターン。  
   - 参照: ** [Query Profile Metrics](./query_profile_operator_metrics.md) ** でメトリクスと Operator の用語集を確認し、** [Query Tuning Recipes](./query_profile_tuning_recipes.md) ** で根本原因の分析を行います。

4. **チューニング戦略の適用**  
   - SQL リライト: SQL クエリをリライトまたは最適化します（例: フィルタを追加、SELECT * を避ける）。  
   - スキーマチューニング: インデックスを追加、テーブルタイプを変更、パーティショニング、クラスタリング。  
   - クエリプランチューニング: 必要に応じてヒントや変数を使用してオプティマイザをガイドします。  
   - 実行チューニング: 特定のワークロードに対してセッション変数をチューニングします。  
   - 参照: ** [Schema Tuning Recipes](./schema_tuning.md) ** でスキーマレベルの最適化を行い、** [Query Hint](./query_hint.md) ** でオプティマイザヒントを確認し、** [Query Tuning Recipes](./query_profile_tuning_recipes.md) ** でプランチューニングと実行チューニングを行います。

5. **検証と反復**  
   - クエリを再実行し、変更前後のパフォーマンスを比較します。  
   - 新しいクエリプランとプロファイルを確認し、改善を確認します。  
   - さらなる最適化が必要な場合は、プロセスを繰り返します。

DBA、開発者、またはデータエンジニアであれ、これらのリソースは以下のことに役立ちます:
- 遅いまたはリソース集約型のクエリを診断し、解決する
- オプティマイザの選択と実行の詳細を理解する
- ベストプラクティスと高度なチューニング戦略を適用する

概要から始め、必要に応じて参考資料に飛び込み、実際のパフォーマンスの課題を解決するためにレシピやヒントを活用してください。