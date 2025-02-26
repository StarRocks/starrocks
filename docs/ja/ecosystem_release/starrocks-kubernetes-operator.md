---
displayed_sidebar: docs
---

# Releases of Kubernetes Operator for StarRocks

## Notifications

StarRocks が提供する Operator は、Kubernetes 環境で StarRocks クラスターをデプロイするために使用されます。StarRocks クラスターのコンポーネントには、FE、BE、CN が含まれます。

**ユーザーガイド:** 以下の方法で Kubernetes 上に StarRocks クラスターをデプロイできます:

- [StarRocks CRD を直接使用して StarRocks クラスターをデプロイする](https://docs.starrocks.io/docs/deployment/sr_operator/)
- [Helm Chart を使用して Operator と StarRocks クラスターの両方をデプロイする](https://docs.starrocks.io/zh/docs/deployment/helm/)

**ソースコード:**

[starrocks-kubernetes-operator と kube-starrocks Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator)

**リソースのダウンロード URL:**

- **URL プレフィックス**

    `https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v${operator_version}/${resource_name}`

- **リソース名**

  - StarRocksCluster CRD: `starrocks.com_starrocksclusters.yaml`
  - StarRocks Operator のデフォルト設定ファイル: `operator.yaml`
  - Helm Chart、`kube-starrocks` Chart `kube-starrocks-${chart_version}.tgz` を含む。`kube-starrocks` Chart は、`starrocks` Chart `starrocks-${chart_version}.tgz` と `operator` Chart `operator-${chart_version}.tgz` の 2 つのサブチャートに分かれています。

例えば、kube-starrocks chart v1.8.6 のダウンロード URL は以下の通りです:

`https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v1.8.6/kube-starrocks-1.8.6.tgz`

**バージョン要件**

- Kubernetes: 1.18 以降
- Go: 1.19 以降

## Release notes

### 1.9

#### 1.9.1

**改善点**

- **[Helm Chart]** `logStorageSize` が `0` に設定されている場合、operator はログストレージ用の PersistentVolumeClaim (PVC) を作成しません。 [#398](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/398)
- **[Operator]** `storageVolumes` 内の `mountPath` と `name` の値が重複しているかどうかを operator が検出できます。重複する値が存在する場合、エラーが返されます。 [#388](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/388)
- **[Operator]** FE ノードの数を 1 にスケールダウンすることはできません。 [#394](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/394)
- **[Operator]** 複数の値 YAML ファイルで `feEnvVars`、`beEnvVars`、`cnEnvVars` フィールドの値をマージできます。 [#396](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/396)
- **[Operator]** StarRocksCluster CRD に `spec.containers.securityContext.capabilities` を追加し、コンテナの Linux 権限をカスタマイズできます。 [#404](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/404)

**バグ修正**

以下の問題を修正しました:

- **[Operator]** `service` 内の `annotations` フィールドを更新できます。 [#402](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/402) [#399](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/399)
- **[Operator]** statefulset と deployment の変更は更新ではなくパッチされます。これにより、CN と HPA が有効になっているときに CN をアップグレードすると、すべての CN ポッドが終了して再起動される問題が解決されます。 [#397](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/397)
- **[Operator]** service オブジェクトの変更は更新ではなくパッチされます。これにより、Kubernetes Cloud プロバイダーが使用され、service オブジェクトがその Kubernetes Cloud プロバイダーによって変更された場合に、operator が変更を上書きするのを防ぎます。 [#387](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/387)

#### 1.9.0

**新機能**

- StarRocks Warehouse をサポートするために StarRocksWarehouse CRD を追加しました。StarRocks Warehouse は現在、StarRocks Enterprise Edition の機能です。 [#323](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/323)

**強化**

- StarRocksCluster CRD に `status.reason` フィールドを追加しました。クラスターのデプロイ中にサブコントローラーの適用操作が失敗した場合、`kubectl get starrockscluster <name_of_the_starrocks_cluster_object> -oyaml` を実行し、返された結果の `status.reason` フィールドに表示されるエラーログを確認できます。 [#359](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/359)
- `storageVolumes` フィールドに空のディレクトリをマウントできます。 [#324](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/324)

**バグ修正**

以下の問題を修正しました:

- StarRocks クラスターのステータスがクラスターの FE、BE、CN コンポーネントのステータスと一致していませんでした。 [#380](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/380)
- `autoScalingPolicy` が削除されたときに HPA リソースが削除されません。 [#379](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/379)
- `starRocksCnSpec` が削除されたときに HPA リソースが削除されません。 [#357](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/357)

### 1.8

#### 1.8.8

**バグ修正**

- **[Operator]** `StarRocksFeSpec.service`、`StarRocksBeSpec.service`、`StarRocksCnSpec.service` を使用してアノテーションが追加された場合、operator は検索サービス（内部サービス）にアノテーションを追加しなくなりました。 [#370](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/370)

#### 1.8.7

**強化**

- StarRocksCluster CRD に `livenessProbeFailureSeconds` と `readinessProbeFailureSeconds` フィールドを追加しました。StarRocks が高負荷の状態にあり、liveness と readiness プローブの時間がデフォルト値を使用している場合、liveness と readiness プローブが失敗し、コンテナが再起動する可能性があります。この場合、これらのフィールドに大きな値を追加できます。 [#309](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/309)

#### 1.8.6

**バグ修正**

以下の問題を修正しました:

- Stream Load ジョブ中に `sendfile() failed (32: Broken pipe) while sending request to upstream` エラーが返されます。Nginx がリクエストボディを FE に送信した後、FE はリクエストを BE にリダイレクトします。この時点で、Nginx にキャッシュされたデータがすでに失われている可能性があります。 [#303](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/303)

**ドキュメント**

- [Kubernetes ネットワーク外から FE プロキシを介して StarRocks にデータをロードする](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/load_data_using_stream_load_howto.md)
- [Helm を使用してルートユーザーのパスワードを更新する](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md)

#### 1.8.5

**改善点**

- **[Helm Chart] Operator のサービスアカウントの `annotations` と `labels` をカスタマイズできます**: Operator はデフォルトで `starrocks` という名前のサービスアカウントを作成し、ユーザーは **values.yaml** の `serviceAccount` 内の `annotations` と `labels` フィールドを指定することで、operator のサービスアカウント `starrocks` のアノテーションとラベルをカスタマイズできます。`operator.global.rbac.serviceAccountName` フィールドは非推奨です。 [#291](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/291)
- **[Operator] FE サービスは Istio のプロトコル選択を明示的にサポートします**: Istio が Kubernetes 環境にインストールされている場合、Istio は StarRocks クラスターからのトラフィックのプロトコルを判断する必要があります。これにより、ルーティングや豊富なメトリクスなどの追加機能を提供できます。そのため、FE サービスは `appProtocol` フィールドでプロトコルを MySQL として明示的に定義します。この改善は特に重要です。なぜなら、MySQL プロトコルはサーバーファーストプロトコルであり、自動プロトコル検出と互換性がなく、接続の失敗を引き起こす可能性があるからです。 [#288](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/288)

**バグ修正**

以下の問題を修正しました:

- **[Helm Chart]** `starrocks.initPassword.enabled` が true で、`starrocks.starrocksCluster.name` の値が指定されている場合、StarRocks のルートユーザーのパスワードが正常に初期化されない可能性があります。これは、initpwd ポッドが FE サービスに接続するために使用する FE サービスドメイン名が間違っていることが原因です。具体的には、このシナリオでは、FE サービスドメイン名は `starrocks.starrocksCluster.name` に指定された値を使用しますが、initpwd ポッドは `starrocks.nameOverride` フィールドの値を使用して FE サービスドメイン名を形成します。 ([#292](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/292))

**アップグレードノート**

- **[Helm Chart]** `starrocks.starrocksCluster.name` に指定された値が `starrocks.nameOverride` の値と異なる場合、FE、BE、CN の古い configmap は削除されます。FE、BE、CN の新しい名前の configmap が作成されます。**これにより、FE、BE、CN ポッドが再起動する可能性があります。**

#### 1.8.4

**機能**

- **[Helm Chart]** StarRocks クラスターのメトリクスを Prometheus と ServiceMonitor CR を使用して監視できます。ユーザーガイドについては、[Integration with Prometheus and Grafana](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-prometheus-grafana.md) を参照してください。 [#284](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/284)
- **[Helm Chart]** **values.yaml** の `starrocksCnSpec` に `storagespec` とその他のフィールドを追加して、StarRocks クラスターの CN ノードのログボリュームを構成します。 [#280](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/280)
- StarRocksCluster CRD に `terminationGracePeriodSeconds` を追加して、StarRocksCluster リソースが削除または更新されるときに、ポッドを強制終了する前に待機する時間を構成します。 [#283](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/283)
- StarRocksCluster CRD に `startupProbeFailureSeconds` フィールドを追加して、StarRocksCluster リソース内のポッドの起動プローブ失敗しきい値を構成します。 [#271](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/271)

**バグ修正**

以下の問題を修正しました:

- StarRocks クラスターに複数の FE ポッドが存在する場合、FE プロキシが STREAM LOAD リクエストを正しく処理できません。 [#269](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/269)

**ドキュメント**

- [ローカル StarRocks クラスターをデプロイするためのクイックスタートを追加](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/local_installation_how_to.md)。
- 異なる構成で StarRocks クラスターをデプロイする方法に関するユーザーガイドを追加しました。例えば、[すべてのサポートされている機能を持つ StarRocks クラスターをデプロイする方法](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/examples/starrocks/deploy_a_starrocks_cluster_with_all_features.yaml)。その他のユーザーガイドについては、[docs](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/examples/starrocks) を参照してください。
- StarRocks クラスターを管理する方法に関するユーザーガイドを追加しました。例えば、[ログと関連フィールドを構成する方法](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/logging_and_related_configurations_howto.md) や [外部 configmap または secret をマウントする方法](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_external_configmaps_or_secrets_howto.md)。その他のユーザーガイドについては、[docs](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/doc) を参照してください。

#### 1.8.3

**アップグレードノート**

- **[Helm Chart]** デフォルトの **fe.conf** ファイルに `JAVA_OPTS_FOR_JDK_11` を追加しました。デフォルトの **fe.conf** ファイルが使用され、helm chart が v1.8.3 にアップグレードされると、**FE ポッドが再起動する可能性があります**。 [#257](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/257)

**機能**

- **[Helm Chart]** Operator が監視する必要がある唯一の名前空間を指定するための `watchNamespace` フィールドを追加しました。そうでない場合、operator は Kubernetes クラスター内のすべての名前空間を監視します。ほとんどの場合、この機能を使用する必要はありません。Kubernetes クラスターがあまりにも多くのノードを管理し、operator がすべての名前空間を監視し、メモリリソースを過剰に消費する場合に、この機能を使用できます。 [#261](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/261)
- **[Helm Chart]** **values.yaml** ファイルの `starrocksFeProxySpec` に `Ports` フィールドを追加し、ユーザーが FE プロキシサービスの NodePort を指定できるようにしました。 [#258](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/258)

**改善点**

- **nginx.conf** ファイルの `proxy_read_timeout` パラメータの値を 60 秒から 600 秒に変更し、タイムアウトを回避します。

#### 1.8.2

**改善点**

- OOM を回避するために、operator ポッドに許可される最大メモリ使用量を増やしました。 [#254](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/254)

#### 1.8.1

**機能**

- [configMaps と secrets の subpath フィールドを使用する](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_external_configmaps_or_secrets_howto.md#3-mount-configmaps-to-a-subpath-by-helm-chart)ことをサポートし、これらのリソースから特定のファイルやディレクトリをマウントできるようにします。 [#249](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/249)
- StarRocks クラスター CRD に `ports` フィールドを追加し、ユーザーがサービスのポートをカスタマイズできるようにしました。 [#244](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/244)

**改善点**

- StarRocks クラスターの `BeSpec` または `CnSpec` が削除されたときに関連する Kubernetes リソースを削除し、クラスターのクリーンで一貫した状態を確保します。 [#245](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/245)

#### 1.8.0

**アップグレードノートと動作の変更**

- **[Operator]** StarRocksCluster CRD と operator をアップグレードするには、新しい StarRocksCluster CRD **starrocks.com_starrocksclusters.yaml** と **operator.yaml** を手動で適用する必要があります。

- **[Helm Chart]**

  - Helm Chart をアップグレードするには、以下を実行する必要があります:

    1. **values migration tool** を使用して、以前の **values.yaml** ファイルの形式を新しい形式に調整します。異なるオペレーティングシステム用の values migration tool は、[Assets](https://github.com/StarRocks/starrocks-kubernetes-operator/releases/tag/v1.8.0) セクションからダウンロードできます。このツールのヘルプ情報は、`migrate-chart-value --help` コマンドを実行することで取得できます。 [#206](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/206)

       ```Bash
       migrate-chart-value --input values.yaml --target-version v1.8.0 --output ./values-v1.8.0.yaml
       ```

    2. Helm Chart リポジトリを更新します。

       ```Bash
       helm repo update
       ```

    3. `helm upgrade` コマンドを実行して、調整された **values.yaml** ファイルを StarRocks helm chart kube-starrocks に適用します。

       ```Bash
       helm upgrade <release-name> starrocks/kube-starrocks -f values-v1.8.0.yaml
       ```

  - 2 つのサブチャート、[operator](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/helm-charts/charts/kube-starrocks/charts/operator) と [starrocks](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/helm-charts/charts/kube-starrocks/charts/starrocks) が kube-starrocks helm chart に追加されました。対応するサブチャートを指定することで、StarRocks operator または StarRocks クラスターをそれぞれインストールすることができます。この方法により、1 つの StarRocks operator と複数の StarRocks クラスターをデプロイするなど、StarRocks クラスターをより柔軟に管理できます。

**機能**

- **[Helm Chart] Kubernetes クラスター内の複数の StarRocks クラスター**。`starrocks` Helm サブチャートをインストールすることで、Kubernetes クラスター内の異なる名前空間に複数の StarRocks クラスターをデプロイすることをサポートします。 [#199](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/199)
- **[Helm Chart]** `helm install` コマンドを実行する際に、StarRocks クラスターのルートユーザーの初期パスワードを構成することをサポートします。この機能は `helm upgrade` コマンドではサポートされていません。
- **[Helm Chart] Datadog との統合:** Datadog と統合して、StarRocks クラスターのメトリクスとログを収集します。この機能を有効にするには、**values.yaml** ファイルで Datadog 関連のフィールドを構成する必要があります。詳細なユーザーガイドについては、[Integration with Datadog](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-with-datadog.md) を参照してください。 [#197](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/197) [#208](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/208)
- **[Operator] 非ルートユーザーとしてポッドを実行**。runAsNonRoot フィールドを追加し、ポッドを非ルートユーザーとして実行できるようにし、セキュリティを強化します。 [#195](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/195)
- **[Operator] FE プロキシ**。FE プロキシを追加し、外部クライアントと Stream Load プロトコルをサポートするデータロードツールが Kubernetes 内の StarRocks クラスターにアクセスできるようにします。この方法により、Stream Load に基づくロードジョブを使用して、Kubernetes 内の StarRocks クラスターにデータをロードできます。 [#211](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/211)

**改善点**

- StarRocksCluster CRD に `subpath` フィールドを追加しました。 [#212](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/212)
- FE メタデータに許可されるディスクサイズを増やしました。FE コンテナは、FE メタデータを保存するためにプロビジョニングできるディスクスペースがデフォルト値より少ない場合に実行を停止します。 [#210](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/210)