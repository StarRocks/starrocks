---
displayed_sidebar: docs
---

# Deploy StarRocks with Helm

[Helm](https://helm.sh/) は Kubernetes のパッケージマネージャーです。[Helm Chart](https://helm.sh/docs/topics/charts/) は Helm パッケージであり、Kubernetes クラスタ上でアプリケーションを実行するために必要なすべてのリソース定義を含んでいます。このトピックでは、Helm を使用して Kubernetes クラスタ上に StarRocks クラスタを自動的にデプロイする方法について説明します。

## 始める前に

- [Kubernetes クラスタを作成する](./sr_operator.md#create-kubernetes-cluster)。
- [Helm をインストールする](https://helm.sh/docs/intro/quickstart/)。

## 手順

1. StarRocks 用の Helm Chart リポジトリを追加します。Helm Chart には StarRocks Operator とカスタムリソース StarRocksCluster の定義が含まれています。
   1. Helm Chart リポジトリを追加します。

      ```Bash
      helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator
      ```

   2. Helm Chart リポジトリを最新バージョンに更新します。

      ```Bash
      helm repo update
      ```

   3. 追加した Helm Chart リポジトリを表示します。

      ```Bash
      $ helm search repo starrocks
      NAME                                    CHART VERSION    APP VERSION  DESCRIPTION
      starrocks/kube-starrocks      1.8.0            3.1-latest   kube-starrocks includes two subcharts, starrock...
      starrocks/operator            1.8.0            1.8.0        A Helm chart for StarRocks operator
      starrocks/starrocks           1.8.0            3.1-latest   A Helm chart for StarRocks cluster
      ```

2. Helm Chart のデフォルト **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** を使用して StarRocks Operator と StarRocks クラスタをデプロイするか、YAML ファイルを作成してデプロイメント設定をカスタマイズします。
   1. デフォルト設定でのデプロイメント

      次のコマンドを実行して、1 つの FE と 1 つの BE からなる StarRocks Operator と StarRocks クラスタをデプロイします。
   > ヒント
   >
   > デフォルトの `values.yaml` は次のように設定されています:
   > - オペレーターポッドは 1/2CPU と 0.8GB RAM
   > - 1 つの FE は 4GB RAM、4 コア、15Gi ディスク
   > - 1 つの BE は 4GB RAM、4 コア、1Ti ディスク
   >
   > これらのリソースが Kubernetes クラスタで利用できない場合は、**カスタム設定でのデプロイメント** セクションに進み、リソースを調整してください。

      ```Bash
      $ helm install starrocks starrocks/kube-starrocks
      # 次の結果が返された場合、StarRocks Operator と StarRocks クラスタがデプロイされています。
      NAME: starrocks
      LAST DEPLOYED: Tue Aug 15 15:12:00 2023
      NAMESPACE: starrocks
      STATUS: deployed
      REVISION: 1
      TEST SUITE: None
      ```

3. カスタム設定でのデプロイメント
   - 例えば **my-values.yaml** という YAML ファイルを作成し、その中で StarRocks Operator と StarRocks クラスタの設定をカスタマイズします。サポートされているパラメータと説明については、Helm Chart のデフォルト **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** のコメントを参照してください。
   - 次のコマンドを実行して、**my-values.yaml** のカスタム設定で StarRocks Operator と StarRocks クラスタをデプロイします。

     ```bash
     helm install -f my-values.yaml starrocks starrocks/kube-starrocks
     ```

    デプロイメントには時間がかかります。この間、次のコマンドでデプロイメントのステータスを確認できます。

    ```bash
    kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
    ```
    次の結果が返された場合、デプロイメントは正常に完了しています。
   
    ```bash
    NAME             PHASE     FESTATUS   BESTATUS   CNSTATUS   FEPROXYSTATUS
    kube-starrocks   running   running    running
    ```

    また、`kubectl get pods` を実行してデプロイメントのステータスを確認することもできます。すべての Pod が `Running` 状態で、Pod 内のすべてのコンテナが `READY` であれば、デプロイメントは正常に完了しています。

    ```bash
    kubectl get pods
    ```

    ```bash
    NAME                                       READY   STATUS    RESTARTS   AGE
    kube-starrocks-be-0                        1/1     Running   0          2m50s
    kube-starrocks-fe-0                        1/1     Running   0          4m31s
    kube-starrocks-operator-69c5c64595-pc7fv   1/1     Running   0          4m50s
    ```

## 次のステップ

- StarRocks クラスタにアクセスする

  Kubernetes クラスタの内外から StarRocks クラスタにアクセスできます。詳細な手順については、[Access StarRocks Cluster](./sr_operator.md#access-starrocks-cluster) を参照してください。

- StarRocks operator と StarRocks クラスタを管理する

  - StarRocks operator と StarRocks クラスタの設定を更新する必要がある場合は、[Helm Upgrade](https://helm.sh/docs/helm/helm_upgrade/) を参照してください。
  - StarRocks Operator と StarRocks クラスタをアンインストールする必要がある場合は、次のコマンドを実行してください。

    ```bash
    helm uninstall starrocks
    ```

## 詳細情報

- GitHub リポジトリのアドレス: [starrocks-kubernetes-operator and kube-starrocks Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator)。

- GitHub リポジトリのドキュメントには、さらに詳しい情報が提供されています。例えば:

  - Kubernetes API を介して StarRocks クラスタのようなオブジェクトを管理する必要がある場合は、[API reference](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/api.md) を参照してください。

  - FE と BE ポッドに永続ボリュームをマウントして、FE のメタデータとログ、および BE のデータとログを保存する必要がある場合は、[Mount Persistent Volumes by Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_persistent_volume_howto.md#2-mounting-persistent-volumes-by-helm-chart) を参照してください。

    :::danger

    永続ボリュームがマウントされていない場合、StarRocks Operator は emptyDir を使用して FE のメタデータとログ、および BE のデータとログを保存します。コンテナが再起動すると、データは失われます。

    :::

  - ルートユーザーのパスワードを設定する必要がある場合:

    - StarRocks クラスタをデプロイした後に手動でルートユーザーのパスワードを設定するには、[Change root user password HOWTO](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md) を参照してください。

    - StarRocks クラスタをデプロイする際にルートユーザーのパスワードを自動的に設定するには、[Initialize root user password](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/initialize_root_password_howto.md) を参照してください。

- StarRocks 共有データクラスタで CREATE TABLE ステートメントを実行した後に発生する次のエラーを解決する方法。

  - **エラーメッセージ**

      ```plaintext
      ERROR 1064 (HY000): Table replication num should be less than or equal to the number of available BE nodes. You can change this default by setting the replication_num table properties. Current alive backend is [10001]. , table=orders1, default_replication_num=3
      ```

  - **解決策**

       これは、StarRocks 共有データクラスタに 1 つの BE しか存在せず、1 つのレプリカのみをサポートしているためかもしれません。しかし、デフォルトのレプリカ数は 3 です。PROPERTIES でレプリカ数を 1 に変更することができます。例えば、`PROPERTIES( "replication_num" = "1" )` のようにします。

- Artifact Hub 上で StarRocks によって維持されている Helm Chart のアドレス: [kube-starrocks](https://artifacthub.io/packages/helm/kube-starrocks/kube-starrocks)。