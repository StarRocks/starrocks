---
displayed_sidebar: docs
---

# Deployment overview

この章では、StarRocks クラスターを本番環境でデプロイ、アップグレード、ダウングレードする方法について説明します。

## Deployment procedure

デプロイ手順の概要は以下の通りで、詳細は後のトピックで説明します。

StarRocks のデプロイは、一般的に以下の手順に従います。

1. StarRocks デプロイメントの[ハードウェアおよびソフトウェア要件](../deployment/deployment_prerequisites.md)を確認します。

   StarRocks をデプロイする前に、サーバーが満たすべき前提条件を確認します。これには、CPU、メモリ、ストレージ、ネットワーク、オペレーティングシステム、および依存関係が含まれます。

2. [クラスターサイズを計画します](../deployment/plan_cluster.md)。

   クラスター内の FE ノードと BE ノードの数、およびサーバーのハードウェア仕様を計画します。

3. [環境設定を確認します](../deployment/environment_configurations.md)。

   サーバーの準備が整ったら、StarRocks をデプロイする前にいくつかの環境設定を確認し、変更する必要があります。

4. [デプロイメントファイルを準備します](../deployment/prepare_deployment_files.md)。

   - x86 アーキテクチャに StarRocks をデプロイしたい場合は、公式ウェブサイトで提供されているソフトウェアパッケージを直接ダウンロードして解凍できます。
   - ARM アーキテクチャに StarRocks をデプロイしたい場合は、StarRocks Docker イメージからデプロイメントファイルを準備する必要があります。
   - Kubernetes に StarRocks をデプロイしたい場合は、このステップをスキップできます。

5. StarRocks をデプロイします。

   - 共有データ StarRocks クラスターをデプロイしたい場合、これはストレージとコンピュートが分離されたアーキテクチャを特徴としています。[Deploy and use shared-data StarRocks](../deployment/shared_data/s3.md) の指示を参照してください。
   - 共有なし StarRocks クラスターをデプロイしたい場合、これはローカルストレージを使用します。以下のオプションがあります：

     - [StarRocks を手動でデプロイします](../deployment/deploy_manually.md)。
     - [Kubernetes 上で Operator を使用して StarRocks をデプロイします](../deployment/sr_operator.md)。
     - [Kubernetes 上で Helm を使用して StarRocks をデプロイします](../deployment/helm.md)。

6. 必要な[デプロイ後のセットアップ](../deployment/post_deployment_setup.md)を行います。

   StarRocks クラスターを本番環境に投入する前に、さらなるセットアップが必要です。これには、初期アカウントのセキュリティ確保や、パフォーマンス関連のシステム変数の設定が含まれます。

## Upgrade and downgrade

既存の StarRocks クラスターを新しいバージョンにアップグレードする予定がある場合、初めて StarRocks をインストールするのではなく、[Upgrade StarRocks](../deployment/upgrade.md) を参照して、アップグレード手順やアップグレード前に考慮すべき問題についての情報を確認してください。

StarRocks クラスターをダウングレードする手順については、[Downgrade StarRocks](../deployment/downgrade.md) を参照してください。