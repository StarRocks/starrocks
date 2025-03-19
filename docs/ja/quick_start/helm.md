---
description: StarRocks をデプロイするために Helm を使用する
displayed_sidebar: docs
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import OperatorPrereqs from '../_assets/deployment/_OperatorPrereqs.mdx'
import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsAllin1.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

# StarRocks with Helm

## 目的

このクイックスタートの目的は次のとおりです:

- Helm を使用して StarRocks Kubernetes Operator と StarRocks クラスターをデプロイする
- StarRocks データベースユーザー `root` のパスワードを設定する
- 3 つの FE と 3 つの BE で高可用性を提供する
- メタデータを永続ストレージに保存する
- データを永続ストレージに保存する
- MySQL クライアントが Kubernetes クラスターの外部から接続できるようにする
- Kubernetes クラスターの外部から Stream Load を使用してデータをロードできるようにする
- 一部の公開データセットをロードする
- データをクエリする

:::tip
データセットとクエリは、Basic Quick Start で使用されるものと同じです。ここでの主な違いは、Helm と StarRocks Operator を使用してデプロイすることです。
:::

使用されるデータは、NYC OpenData と National Centers for Environmental Information によって提供されています。

これらのデータセットはどちらも大規模であり、このチュートリアルは StarRocks を使用する経験を得ることを目的としているため、過去 120 年分のデータをロードすることはありません。3 台の e2-standard-4 マシン（または同等のもの）で構築された GKE Kubernetes クラスターでこれを実行できます。より大規模なデプロイメントについては、他のドキュメントを用意しており、後で提供します。

このドキュメントには多くの情報が含まれており、最初にステップバイステップの内容が提示され、最後に技術的な詳細が示されています。これは次の目的を順に果たすために行われています:

1. Helm でシステムをデプロイする。
2. 読者が StarRocks にデータをロードし、そのデータを分析できるようにする。
3. ロード中のデータ変換の基本を説明する。

---

## 前提条件

<OperatorPrereqs />

### SQL クライアント

Kubernetes 環境で提供される SQL クライアントを使用するか、システム上のものを使用できます。このガイドでは `mysql CLI` を使用します。多くの MySQL 互換クライアントが動作します。

### curl

`curl` は StarRocks にデータロードジョブを発行し、データセットをダウンロードするために使用されます。OS プロンプトで `curl` または `curl.exe` を実行して、インストールされているか確認してください。curl がインストールされていない場合は、[こちらから curl を入手してください](https://curl.se/dlwiz/?type=bin)。

---

## 用語

### FE

フロントエンドノードは、メタデータ管理、クライアント接続管理、クエリプランニング、クエリスケジューリングを担当します。各 FE はメモリ内にメタデータの完全なコピーを保持し、FE 間でのサービスの無差別性を保証します。

### BE

バックエンドノードは、データストレージとクエリプランの実行の両方を担当します。

---

## StarRocks Helm チャートリポジトリを追加する

Helm Chart には StarRocks Operator とカスタムリソース StarRocksCluster の定義が含まれています。
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
      helm search repo starrocks
      ```

      ```
      NAME                              	CHART VERSION	APP VERSION	DESCRIPTION
      starrocks/kube-starrocks	1.9.7        	3.2-latest 	kube-starrocks includes two subcharts, operator...
      starrocks/operator      	1.9.7        	1.9.7      	A Helm chart for StarRocks operator
      starrocks/starrocks     	1.9.7        	3.2-latest 	A Helm chart for StarRocks cluster
      starrocks/warehouse     	1.9.7        	3.2-latest 	Warehouse is currently a feature of the StarRoc...
      ```

---

## データをダウンロードする

これらの 2 つのデータセットをマシンにダウンロードします。

### ニューヨーク市のクラッシュデータ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### 気象データ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## Helm の values ファイルを作成する

このクイックスタートの目標は次のとおりです:

1. StarRocks データベースユーザー `root` のパスワードを設定する
2. 3 つの FE と 3 つの BE で高可用性を提供する
3. メタデータを永続ストレージに保存する
4. データを永続ストレージに保存する
5. MySQL クライアントが Kubernetes クラスターの外部から接続できるようにする
6. Kubernetes クラスターの外部から Stream Load を使用してデータをロードできるようにする

Helm チャートはこれらの目標を満たすオプションを提供しますが、デフォルトでは設定されていません。このセクションの残りの部分では、これらの目標をすべて満たすために必要な設定を説明します。完全な values スペックが提供されますが、最初に 6 つのセクションの詳細を読み、次に完全なスペックをコピーしてください。

### 1. データベースユーザーのパスワード

この YAML の一部は、StarRocks operator に対して、Kubernetes secret `starrocks-root-pass` の `password` キーの値にデータベースユーザー `root` のパスワードを設定するよう指示します。

```yaml
starrocks:
    initPassword:
        enabled: true
        # Set a password secret, for example:
        # kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
        passwordSecret: starrocks-root-pass
```

- タスク: Kubernetes secret を作成する

    ```bash
    kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
    ```

### 2. 3 つの FE と 3 つの BE での高可用性

`starrocks.starrockFESpec.replicas` を 3 に設定し、`starrocks.starrockBeSpec.replicas` を 3 に設定することで、高可用性のための十分な FE と BE を持つことができます。CPU とメモリのリクエストを低く設定することで、小規模な Kubernetes 環境でポッドを作成できるようにします。

```yaml
starrocks:
    starrocksFESpec:
        replicas: 3
        resources:
            requests:
                cpu: 1
                memory: 1Gi

    starrocksBeSpec:
        replicas: 3
        resources:
            requests:
                cpu: 1
                memory: 2Gi
```

### 3. メタデータを永続ストレージに保存する

`starrocks.starrocksFESpec.storageSpec.name` に `""` 以外の値を設定すると、次のことが発生します:
- 永続ストレージが使用される
- `starrocks.starrocksFESpec.storageSpec.name` の値がサービスのすべてのストレージボリュームのプレフィックスとして使用される

値を `fe` に設定することで、これらの PV が FE 0 のために作成されます:

- `fe-meta-kube-starrocks-fe-0`
- `fe-log-kube-starrocks-fe-0`

```yaml
starrocks:
    starrocksFESpec:
        storageSpec:
            name: fe
```

### 4. データを永続ストレージに保存する

`starrocks.starrocksBeSpec.storageSpec.name` に `""` 以外の値を設定すると、次のことが発生します:
- 永続ストレージが使用される
- `starrocks.starrocksBeSpec.storageSpec.name` の値がサービスのすべてのストレージボリュームのプレフィックスとして使用される

値を `be` に設定することで、これらの PV が BE 0 のために作成されます:

- `be-data-kube-starrocks-be-0`
- `be-log-kube-starrocks-be-0`

`storageSize` を 15Gi に設定することで、デフォルトの 1Ti からストレージを削減し、ストレージの小さなクォータに適合させます。

```yaml
starrocks:
    starrocksBeSpec:
        storageSpec:
            name: be
            storageSize: 15Gi
```

### 5. MySQL クライアント用の LoadBalancer

デフォルトでは、FE サービスへのアクセスはクラスター IP を通じて行われます。外部アクセスを許可するために、`service.type` を `LoadBalancer` に設定します。

```yaml
starrocks:
    starrocksFESpec:
        service:
            type: LoadBalancer
```

### 6. 外部データロード用の LoadBalancer

Stream Load は、FE と BE の両方への外部アクセスを必要とします。リクエストは FE に送信され、FE はアップロードを処理する BE を割り当てます。`curl` コマンドが BE にリダイレクトされるようにするために、`starroclFeProxySpec` を有効にして `LoadBalancer` タイプに設定する必要があります。

```yaml
starrocks:
    starrocksFeProxySpec:
        enabled: true
        service:
            type: LoadBalancer
```

### 完全な values ファイル

上記のスニペットを組み合わせて、完全な values ファイルを提供します。これを `my-values.yaml` に保存してください:

```yaml
starrocks:
    initPassword:
        enabled: true
        # Set a password secret, for example:
        # kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
        passwordSecret: starrocks-root-pass

    starrocksFESpec:
        replicas: 3
        service:
            type: LoadBalancer
        resources:
            requests:
                cpu: 1
                memory: 1Gi
        storageSpec:
            name: fe

    starrocksBeSpec:
        replicas: 3
        resources:
            requests:
                cpu: 1
                memory: 2Gi
        storageSpec:
            name: be
            storageSize: 15Gi

    starrocksFeProxySpec:
        enabled: true
        service:
            type: LoadBalancer
```

## StarRocks root データベースユーザーのパスワードを設定する

Kubernetes クラスターの外部からデータをロードするために、StarRocks データベースは外部に公開されます。StarRocks データベースユーザー `root` のパスワードを設定する必要があります。オペレーターは FE と BE ノードにパスワードを適用します。

```bash
kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
```

```
secret/starrocks-root-pass created
```
---

## オペレーターと StarRocks クラスターをデプロイする

```bash
helm install -f my-values.yaml starrocks starrocks/kube-starrocks
```

```
NAME: starrocks
LAST DEPLOYED: Wed Jun 26 20:25:09 2024
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Thank you for installing kube-starrocks-1.9.7 kube-starrocks chart.
It will install both operator and starrocks cluster, please wait for a few minutes for the cluster to be ready.

Please see the values.yaml for more operation information: https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml
```

## StarRocks クラスターのステータスを確認する

次のコマンドで進行状況を確認できます:

```bash
kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
```

```
NAME             PHASE         FESTATUS      BESTATUS      CNSTATUS   FEPROXYSTATUS
kube-starrocks   reconciling   reconciling   reconciling              reconciling
```

```bash
kubectl get pods
```

:::note
`kube-starrocks-initpwd` ポッドは、StarRocks root パスワードを設定するために FE および BE ポッドに接続しようとする際に、`error` および `CrashLoopBackOff` 状態を経ることがあります。これらのエラーは無視し、このポッドのステータスが `Completed` になるのを待つべきです。
:::

```
NAME                                       READY   STATUS             RESTARTS      AGE
kube-starrocks-be-0                        0/1     Running            0             20s
kube-starrocks-be-1                        0/1     Running            0             20s
kube-starrocks-be-2                        0/1     Running            0             20s
kube-starrocks-fe-0                        1/1     Running            0             66s
kube-starrocks-fe-1                        0/1     Running            0             65s
kube-starrocks-fe-2                        0/1     Running            0             66s
kube-starrocks-fe-proxy-56f8998799-d4qmt   1/1     Running            0             20s
kube-starrocks-initpwd-m84br               0/1     CrashLoopBackOff   3 (50s ago)   92s
kube-starrocks-operator-54ffcf8c5c-xsjc8   1/1     Running            0             92s
```

```bash
kubectl get pvc
```

```
NAME                          STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   VOLUMEATTRIBUTESCLASS   AGE
be-data-kube-starrocks-be-0   Bound    pvc-4ae0c9d8-7f9a-4147-ad74-b22569165448   15Gi       RWO            standard-rwo   <unset>                 82s
be-data-kube-starrocks-be-1   Bound    pvc-28b4dbd1-0c8f-4b06-87e8-edec616cabbc   15Gi       RWO            standard-rwo   <unset>                 82s
be-data-kube-starrocks-be-2   Bound    pvc-c7232ea6-d3d9-42f1-bfc1-024205a17656   15Gi       RWO            standard-rwo   <unset>                 82s
be-log-kube-starrocks-be-0    Bound    pvc-6193c43d-c74f-4d12-afcc-c41ace3d5408   1Gi        RWO            standard-rwo   <unset>                 82s
be-log-kube-starrocks-be-1    Bound    pvc-c01f124a-014a-439a-99a6-6afe95215bf0   1Gi        RWO            standard-rwo   <unset>                 82s
be-log-kube-starrocks-be-2    Bound    pvc-136df15f-4d2e-43bc-a1c0-17227ce3fe6b   1Gi        RWO            standard-rwo   <unset>                 82s
fe-log-kube-starrocks-fe-0    Bound    pvc-7eac524e-d286-4760-b21c-d9b6261d976f   5Gi        RWO            standard-rwo   <unset>                 2m23s
fe-log-kube-starrocks-fe-1    Bound    pvc-38076b78-71e8-4659-b8e7-6751bec663f6   5Gi        RWO            standard-rwo   <unset>                 2m23s
fe-log-kube-starrocks-fe-2    Bound    pvc-4ccfee60-02b7-40ba-a22e-861ea29dac74   5Gi        RWO            standard-rwo   <unset>                 2m23s
fe-meta-kube-starrocks-fe-0   Bound    pvc-5130c9ff-b797-4f79-a1d2-4214af860d70   10Gi       RWO            standard-rwo   <unset>                 2m23s
fe-meta-kube-starrocks-fe-1   Bound    pvc-13545330-63be-42cf-b1ca-3ed6f96a8c98   10Gi       RWO            standard-rwo   <unset>                 2m23s
fe-meta-kube-starrocks-fe-2   Bound    pvc-609cadd4-c7b7-4cf9-84b0-a75678bb3c4d   10Gi       RWO            standard-rwo   <unset>                 2m23s
```
### クラスターが正常であることを確認する

:::tip
これらは上記と同じコマンドですが、望ましい状態を示しています。
:::

```bash
kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
```

```
NAME             PHASE     FESTATUS   BESTATUS   CNSTATUS   FEPROXYSTATUS
kube-starrocks   running   running    running               running
```

```bash
kubectl get pods
```

:::tip
システムは、`kube-starrocks-initpwd` を除くすべてのポッドが `READY` 列で `1/1` を示したときに準備が整います。`kube-starrocks-initpwd` ポッドは `0/1` を示し、`STATUS` が `Completed` であるべきです。
:::

```
NAME                                       READY   STATUS      RESTARTS   AGE
kube-starrocks-be-0                        1/1     Running     0          57s
kube-starrocks-be-1                        1/1     Running     0          57s
kube-starrocks-be-2                        1/1     Running     0          57s
kube-starrocks-fe-0                        1/1     Running     0          103s
kube-starrocks-fe-1                        1/1     Running     0          102s
kube-starrocks-fe-2                        1/1     Running     0          103s
kube-starrocks-fe-proxy-56f8998799-d4qmt   1/1     Running     0          57s
kube-starrocks-initpwd-m84br               0/1     Completed   4          2m9s
kube-starrocks-operator-54ffcf8c5c-xsjc8   1/1     Running     0          2m9s
```

ハイライトされた行の `EXTERNAL-IP` アドレスは、Kubernetes クラスターの外部からの SQL クライアントと Stream Load アクセスを提供するために使用されます。

```bash
kubectl get services
```

```bash
NAME                              TYPE           CLUSTER-IP       EXTERNAL-IP     PORT(S)                                                       AGE
kube-starrocks-be-search          ClusterIP      None             <none>          9050/TCP                                                      78s
kube-starrocks-be-service         ClusterIP      34.118.228.231   <none>          9060/TCP,8040/TCP,9050/TCP,8060/TCP                           78s
# highlight-next-line
kube-starrocks-fe-proxy-service   LoadBalancer   34.118.230.176   34.176.12.205   8080:30241/TCP                                                78s
kube-starrocks-fe-search          ClusterIP      None             <none>          9030/TCP                                                      2m4s
# highlight-next-line
kube-starrocks-fe-service         LoadBalancer   34.118.226.82    34.176.215.97   8030:30620/TCP,9020:32461/TCP,9030:32749/TCP,9010:30911/TCP   2m4s
kubernetes                        ClusterIP      34.118.224.1     <none>          443/TCP                                                       8h
```

:::tip
ハイライトされた行の `EXTERNAL-IP` アドレスを環境変数に保存して、手元に置いておくと便利です:

```
export MYSQL_IP=`kubectl get services kube-starrocks-fe-service --output jsonpath='{.status.loadBalancer.ingress[0].ip}'`
```
```
export FE_PROXY=`kubectl get services kube-starrocks-fe-proxy-service --output jsonpath='{.status.loadBalancer.ingress[0].ip}'`:8080
```
:::

---

### SQL クライアントで StarRocks に接続する

:::tip

mysql CLI 以外のクライアントを使用している場合は、今すぐ開いてください。
:::

このコマンドは Kubernetes ポッドで `mysql` コマンドを実行します:

```sql
kubectl exec --stdin --tty kube-starrocks-fe-0 -- \
  mysql -P9030 -h127.0.0.1 -u root --prompt="StarRocks > "
```

mysql CLI がローカルにインストールされている場合は、Kubernetes クラスター内のものではなく、それを使用できます:

```sql
mysql -P9030 -h $MYSQL_IP -u root --prompt="StarRocks > " -p
```

---
## テーブルを作成する

```bash
mysql -P9030 -h $MYSQL_IP -u root --prompt="StarRocks > " -p
```

<DDL />

MySQL クライアントから退出するか、コマンドラインでコマンドを実行するために新しいシェルを開いてデータをアップロードします。

```sql
exit
```

## データをアップロードする

StarRocks にデータをロードする方法は多数あります。このチュートリアルでは、最も簡単な方法は curl と StarRocks Stream Load を使用することです。

先ほどダウンロードした 2 つのデータセットをアップロードします。

:::tip
これらの curl コマンドは、`mysql` クライアントではなく、オペレーティングシステムのプロンプトで実行されるため、新しいシェルを開いてください。コマンドはダウンロードしたデータセットを参照しているため、ファイルをダウンロードしたディレクトリから実行してください。

これは新しいシェルなので、エクスポートコマンドを再度実行してください:

```bash

export MYSQL_IP=`kubectl get services kube-starrocks-fe-service --output jsonpath='{.status.loadBalancer.ingress[0].ip}'`

export FE_PROXY=`kubectl get services kube-starrocks-fe-proxy-service --output jsonpath='{.status.loadBalancer.ingress[0].ip}'`:8080
```

パスワードを求められます。Kubernetes secret `starrocks-root-pass` に追加したパスワードを使用してください。提供されたコマンドを使用した場合、パスワードは `g()()dpa$$word` です。
:::

`curl` コマンドは複雑に見えますが、チュートリアルの最後に詳細に説明されています。今はコマンドを実行し、データを分析するために SQL を実行し、その後データロードの詳細を読むことをお勧めします。

```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    # highlight-next-line
    -XPUT http://$FE_PROXY/api/quickstart/crashdata/_stream_load
```

```
Enter host password for user 'root':
{
    "TxnId": 2,
    "Label": "crashdata-0",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 423726,
    "NumberLoadedRows": 423725,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 96227746,
    "LoadTimeMs": 2483,
    "BeginTxnTimeMs": 42,
    "StreamLoadPlanTimeMs": 122,
    "ReadDataTimeMs": 1610,
    "WriteDataTimeMs": 2253,
    "CommitAndPublishTimeMs": 65,
    "ErrorURL": "http://kube-starrocks-be-2.kube-starrocks-be-search.default.svc.cluster.local:8040/api/_load_error_log?file=error_log_5149e6f80de42bcb_eab2ea77276de4ba"
}
```

```bash
curl --location-trusted -u root             \
    -T ./72505394728.csv                    \
    -H "label:weather-0"                    \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate" \
    # highlight-next-line
    -XPUT http://$FE_PROXY/api/quickstart/weatherdata/_stream_load
```

```
Enter host password for user 'root':
{
    "TxnId": 4,
    "Label": "weather-0",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 22931,
    "NumberLoadedRows": 22931,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 15558550,
    "LoadTimeMs": 404,
    "BeginTxnTimeMs": 1,
    "StreamLoadPlanTimeMs": 7,
    "ReadDataTimeMs": 157,
    "WriteDataTimeMs": 372,
    "CommitAndPublishTimeMs": 23
}
```

## MySQL クライアントで接続する

接続していない場合は、MySQL クライアントで接続してください。`kube-starrocks-fe-service` サービスの外部 IP アドレスと、Kubernetes secret `starrocks-root-pass` に設定したパスワードを使用することを忘れないでください。

```bash
mysql -P9030 -h $MYSQL_IP -u root --prompt="StarRocks > " -p
```

## 質問に答える

<SQL />

```sql
exit
```

## クリーンアップ

終了して StarRocks クラスターと StarRocks オペレーターを削除したい場合は、このコマンドを実行します。

```bash
helm delete starrocks
```

---

## まとめ

このチュートリアルでは、次のことを行いました:

- Helm と StarRocks Operator を使用して StarRocks をデプロイしました
- ニューヨーク市が提供するクラッシュデータと NOAA が提供する気象データをロードしました
- SQL JOIN を使用して、視界が悪い状態や氷結した道路での運転が悪いアイデアであることを分析しました

学ぶべきことはまだあります。Stream Load 中に行われたデータ変換については意図的に詳しく説明しませんでした。curl コマンドに関する詳細は以下のノートにあります。

---

## curl コマンドに関するノート

<Curl />

---

## 詳細情報

デフォルトの [`values.yaml`](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)

[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)

[Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) データセットは、ニューヨーク市によって提供され、これらの[利用規約](https://www.nyc.gov/home/terms-of-use.page)および[プライバシーポリシー](https://www.nyc.gov/home/privacy-policy.page)に従います。

[Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD) は、NOAA によって提供され、[免責事項](https://www.noaa.gov/disclaimer)および[プライバシーポリシー](https://www.noaa.gov/protecting-your-privacy)に従います。

[Helm](https://helm.sh/) は Kubernetes のパッケージマネージャーです。[Helm Chart](https://helm.sh/docs/topics/charts/) は Helm パッケージであり、Kubernetes クラスターでアプリケーションを実行するために必要なすべてのリソース定義を含んでいます。

[`starrocks-kubernetes-operator` と `kube-starrocks` Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator).