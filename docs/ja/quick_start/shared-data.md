---
description: 計算とストレージの分離
displayed_sidebar: docs
sidebar_position: 2
description: Separate compute and storage
---

# ストレージとコンピューティングの分離

import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsCompose.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

ストレージとコンピュートを分離したシステムでは、データは Amazon S3、Google Cloud Storage、Azure Blob Storage、MinIO などの S3 互換ストレージのような低コストで信頼性の高いリモートストレージシステムに保存されます。ホットデータはローカルにキャッシュされ、キャッシュがヒットすると、クエリパフォーマンスはストレージとコンピュートが結合されたアーキテクチャと同等になります。コンピュートノード (CN) は、数秒でオンデマンドで追加または削除できます。このアーキテクチャは、ストレージコストを削減し、リソースの分離を改善し、弾力性とスケーラビリティを提供します。

このチュートリアルでは以下をカバーします:

- Docker コンテナでの StarRocks の実行
- オブジェクトストレージに MinIO を使用する
- 共有データ用に StarRocks を構成する
- 2 つの公開データセットのロード
- SELECT と JOIN を使用したデータの分析
- 基本的なデータ変換 (ETL の **T**)

使用されるデータは、NYC OpenData と NOAA の National Centers for Environmental Information によって提供されています。

これらのデータセットは非常に大きいため、このチュートリアルは StarRocks を使用する経験を得ることを目的としており、過去 120 年分のデータをロードすることはありません。Docker イメージを実行し、このデータを Docker に 4 GB の RAM を割り当てたマシンでロードできます。より大規模でフォールトトレラントなスケーラブルなデプロイメントについては、他のドキュメントを用意しており、後で提供します。

このドキュメントには多くの情報が含まれており、最初にステップバイステップの内容が提示され、最後に技術的な詳細が示されています。これは次の目的を順に果たすためです:

1. 読者が共有データデプロイメントにデータをロードし、そのデータを分析できるようにする。
2. 共有データデプロイメントの構成の詳細を提供する。
3. ロード中のデータ変換の基本を説明する。

---

## 前提条件

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- Docker に割り当てられた 4 GB の RAM
- Docker に割り当てられた 10 GB の空きディスク容量

### SQL クライアント

Docker 環境で提供される SQL クライアントを使用するか、システム上のクライアントを使用できます。多くの MySQL 互換クライアントが動作し、このガイドでは DBeaver と MySQL Workbench の設定をカバーします。

### curl

`curl` は StarRocks にデータロードジョブを発行し、データセットをダウンロードするために使用されます。OS のプロンプトで `curl` または `curl.exe` を実行してインストールされているか確認してください。curl がインストールされていない場合は、[こちらから curl を取得してください](https://curl.se/dlwiz/?type=bin) 。

### `/etc/hosts`

このガイドで使用されるインジェスト方法は Stream Load です。Stream Load は FE サービスに接続してインジェストジョブを開始します。FE はその後、ジョブをバックエンドノード、つまりこのガイドでは CN に割り当てます。インジェストジョブが CN に接続するためには、CN の名前がオペレーティングシステムに認識されている必要があります。`/etc/hosts` に次の行を追加してください:

```bash
127.0.0.1 starrocks-cn
```

---

## 用語

### FE

Frontend ノードは、メタデータ管理、クライアント接続管理、クエリプランニング、およびクエリスケジューリングを担当します。各 FE は、メタデータの完全なコピーをメモリに保存および維持し、FE 間で無差別なサービスを保証します。

### CN

コンピュートノードは、共有データデプロイメントでクエリプランを実行する役割を担います。

### BE

バックエンドノードは、共有なしデプロイメントでデータストレージとクエリプランの実行の両方を担当します。

:::note
このガイドでは BE は使用しません。この情報は、BE と CN の違いを理解できるようにするために含まれています。
:::

---

## ホストファイルを編集する

このガイドで使用されるインジェスト方法は Stream Load です。Stream Load は FE サービスに接続してインジェストジョブを開始します。FE はその後、ジョブをバックエンドノード、つまりこのガイドでは CN に割り当てます。インジェストジョブが CN に接続するためには、CN の名前がオペレーティングシステムに認識されている必要があります。`/etc/hosts` に次の行を追加してください:

```bash
127.0.0.1 starrocks-cn
```

## ラボファイルをダウンロードする

ダウンロードするファイルは 3 つあります:

- StarRocks と MinIO 環境をデプロイする Docker Compose ファイル
- ニューヨーク市の衝突データ
- 天気データ

このガイドでは、GNU Affero General Public License の下で提供される S3 互換のオブジェクトストレージである MinIO を使用します。

### ラボファイルを保存するディレクトリを作成する

```bash
mkdir quickstart
cd quickstart
```

### Docker Compose ファイルをダウンロードする

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

### データをダウンロードする

次の 2 つのデータセットをダウンロードします:

#### ニューヨーク市の衝突データ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

#### 天気データ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## StarRocks と MinIO をデプロイする

```bash
docker compose up --detach --wait --wait-timeout 120
```

FE、CN、MinIO サービスが正常になるまで約 30 秒かかります。`quickstart-minio_mc-1` コンテナは `Waiting` のステータスを示し、終了コードも表示されます。終了コードが `0` の場合は成功を示します。

```bash
[+] Running 4/5
 ✔ Network quickstart_default       Created    0.0s
 ✔ Container minio                  Healthy    6.8s
 ✔ Container starrocks-fe           Healthy    29.3s
 ⠼ Container quickstart-minio_mc-1  Waiting    29.3s
 ✔ Container starrocks-cn           Healthy    29.2s
container quickstart-minio_mc-1 exited (0)
```

---

## MinIO

このクイックスタートでは、共有ストレージとして MinIO を使用します。

### MinIO のクレデンシャルを確認する

StarRocks で MinIO をオブジェクトストレージとして使用するには、StarRocks に MinIO のアクセスキーが必要です。アクセスキーは Docker サービスの起動時に生成されました。StarRocks が MinIO に接続する方法をよりよく理解するために、キーが存在することを確認してください。

[http://localhost:9001/access-keys](http://localhost:9001/access-keys) にアクセスします。ユーザー名とパスワードは Docker compose ファイルに指定されており、`miniouser` と `miniopassword` です。1 つのアクセスキーがあることが確認できます。キーは `AAAAAAAAAAAAAAAAAAAA` で、MinIO コンソールではシークレットは表示されませんが、Docker compose ファイルには `BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB` と記載されています。

![MinIO アクセスキーを表示](../_assets/quick-start/MinIO-view-key.png)

:::tip
MinIO の Web UI にアクセスキーが表示されない場合は、`minio_mc` サービスのログを確認してください:

```bash
docker compose logs minio_mc
```

`minio_mc` ポッドを再実行してみてください:

```bash
docker compose run minio_mc
```
:::

### データ用のバケットを作成する

StarRocks でストレージボリュームを作成する際に、データの `LOCATION` を指定します:

```sh
    LOCATIONS = ("s3://my-starrocks-bucket/")
```

[http://localhost:9001/buckets](http://localhost:9001/buckets) を開き、ストレージボリュームのバケットを追加します。バケットに `my-starrocks-bucket` という名前を付けます。リストされている 3 つのオプションのデフォルトを受け入れます。

---

## SQL クライアント

<Clients />

---

## 共有データ用の StarRocks 構成

この時点で StarRocks が稼働しており、MinIO も稼働しています。MinIO アクセスキーは StarRocks と MinIO を接続するために使用されます。

これは、StarRocks デプロイメントが共有データを使用することを指定する `FE` 設定の一部です。これは Docker Compose がデプロイメントを作成した際に `fe.conf` ファイルに追加されました。

```sh
# enable the shared data run mode
run_mode = shared_data
cloud_native_storage_type = S3
```

:::info
これらの設定を確認するには、`quickstart` ディレクトリからこのコマンドを実行し、ファイルの末尾を確認してください:
:::

```sh
docker compose exec starrocks-fe \
  cat /opt/starrocks/fe/conf/fe.conf
```
:::

### SQL クライアントで StarRocks に接続する

:::tip

`docker-compose.yml` ファイルを含むディレクトリからこのコマンドを実行します。

MySQL コマンドラインクライアント以外のクライアントを使用している場合は、今すぐ開いてください。
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

#### ストレージボリュームを確認する

```sql
SHOW STORAGE VOLUMES;
```

:::tip
ストレージボリュームは存在しないはずです。次に作成します。
:::

```sh
Empty set (0.04 sec)
```

#### 共有データストレージボリュームを作成する

以前に、MinIO に `my-starrocks-volume` という名前のバケットを作成し、MinIO に `AAAAAAAAAAAAAAAAAAAA` という名前のアクセスキーがあることを確認しました。次の SQL は、アクセスキーとシークレットを使用して、MinIO バケットにストレージボリュームを作成します。

```sql
CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES
    (
         "enabled" = "true",
         "aws.s3.endpoint" = "minio:9000",
         "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
         "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
         "aws.s3.use_instance_profile" = "false",
         "aws.s3.use_aws_sdk_default_behavior" = "false"
     );
```

これで、ストレージボリュームがリストされるはずです。以前は空のセットでした:

```
SHOW STORAGE VOLUMES;
```

```
+----------------+
| Storage Volume |
+----------------+
| s3_volume      |
+----------------+
1 row in set (0.02 sec)
```

ストレージボリュームの詳細を表示し、まだデフォルトボリュームではなく、バケットを使用するように設定されていることを確認します:

```
DESC STORAGE VOLUME s3_volume\G
```

:::tip
このドキュメントの一部の SQL および StarRocks ドキュメントの多くの他のドキュメントでは、セミコロンの代わりに `\G` で終わります。`mysql` CLI でクエリ結果を縦に表示するために `\G` を使用します。

多くの SQL クライアントは縦のフォーマット出力を解釈しないため、`\G` を `;` に置き換える必要があります。
:::

```sh
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
# highlight-start
IsDefault: false
 Location: s3://my-starrocks-bucket/
# highlight-end
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
1 row in set (0.02 sec)
```

## デフォルトストレージボリュームを設定する

```
SET s3_volume AS DEFAULT STORAGE VOLUME;
```

```
DESC STORAGE VOLUME s3_volume\G
```

```sh
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
# highlight-next-line
IsDefault: true
 Location: s3://my-starrocks-bucket/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
1 row in set (0.02 sec)
```

## データベースを作成する

```
CREATE DATABASE IF NOT EXISTS quickstart;
```

データベース `quickstart` がストレージボリューム `s3_volume` を使用していることを確認します:

```
SHOW CREATE DATABASE quickstart \G
```

```sh
*************************** 1. row ***************************
       Database: quickstart
Create Database: CREATE DATABASE `quickstart`
# highlight-next-line
PROPERTIES ("storage_volume" = "s3_volume")
```

---

## テーブルを作成する

<DDL />

---

## 2 つのデータセットをロードする

StarRocks にデータをロードする方法はたくさんあります。このチュートリアルでは、最も簡単な方法は curl と StarRocks Stream Load を使用することです。

:::tip

データセットをダウンロードしたディレクトリからこれらの curl コマンドを実行します。

パスワードを求められます。おそらく MySQL の `root` ユーザーにパスワードを設定していないため、Enter を押すだけで大丈夫です。

:::

`curl` コマンドは複雑に見えますが、チュートリアルの最後で詳細に説明されています。今はコマンドを実行してデータを分析するための SQL を実行し、その後にデータロードの詳細を読むことをお勧めします。

### ニューヨーク市の衝突データ - 衝突

```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    -XPUT http://localhost:8030/api/quickstart/crashdata/_stream_load
```

上記のコマンドの出力です。最初のハイライトされたセクションは、期待される結果 (OK と 1 行を除くすべての行が挿入されたこと) を示しています。1 行は列数が正しくないためフィルタリングされました。

```bash
Enter host password for user 'root':
{
    "TxnId": 2,
    "Label": "crashdata-0",
    "Status": "Success",
    # highlight-start
    "Message": "OK",
    "NumberTotalRows": 423726,
    "NumberLoadedRows": 423725,
    # highlight-end
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 96227746,
    "LoadTimeMs": 1013,
    "BeginTxnTimeMs": 21,
    "StreamLoadPlanTimeMs": 63,
    "ReadDataTimeMs": 563,
    "WriteDataTimeMs": 870,
    "CommitAndPublishTimeMs": 57,
    # highlight-start
    "ErrorURL": "http://starrocks-cn:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

エラーが発生した場合、出力にはエラーメッセージを確認するための URL が提供されます。エラーメッセージには、Stream Load ジョブが割り当てられたバックエンドノード (`starrocks-cn`) も含まれています。`/etc/hosts` ファイルに `starrocks-cn` のエントリを追加したため、そこに移動してエラーメッセージを読むことができるはずです。

このチュートリアルの開発中に見られた内容の要約を展開します:

<details>

<summary>ブラウザでエラーメッセージを読む</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.

Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### 天気データ

クラッシュデータをロードしたのと同じ方法で気象データセットをロードします。

```bash
curl --location-trusted -u root             \
    -T ./72505394728.csv                    \
    -H "label:weather-0"                    \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate" \
    -XPUT http://localhost:8030/api/quickstart/weatherdata/_stream_load
```

---

## MinIO にデータが保存されていることを確認する

MinIO を開き、[http://localhost:9001/browser/my-starrocks-bucket](http://localhost:9001/browser/my-starrocks-bucket) にアクセスして、`my-starrocks-bucket/` 以下にエントリがあることを確認します。

:::tip
`my-starrocks-bucket/` 以下のフォルダ名はデータをロードした際に生成されます。`my-starrocks-bucket` の下に単一のディレクトリがあり、その下にさらに 2 つのディレクトリがあるはずです。それらのディレクトリにはデータ、メタデータ、またはスキーマエントリが含まれています。

![MinIO オブジェクトブラウザ](../_assets/quick-start/MinIO-data.png)
:::

---

## いくつかの質問に答える

<SQL />

---

## 共有データ用に StarRocks を構成する

StarRocks を共有データで使用する経験を得た今、設定を理解することが重要です。

### CN 構成

ここで使用される CN 設定はデフォルトです。CN は共有データの使用を目的として設計されています。デフォルトの設定は以下の通りです。変更を加える必要はありません。

```bash
sys_log_level = INFO

# ports for admin, web, heartbeat service
be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
starlet_port = 9070
```

### FE 構成

FE 設定はデフォルトとは少し異なります。FE はデータが BE ノードのローカルディスクではなくオブジェクトストレージに保存されることを期待するように設定されている必要があります。

`docker-compose.yml` ファイルは `command` 内で FE 設定を生成します。

```plaintext
# enable shared data, set storage type, set endpoint
run_mode = shared_data
cloud_native_storage_type = S3
```

:::note
この構成ファイルには、FE のデフォルトエントリは含まれていません。共有データ構成のみが表示されます。
:::

デフォルトではない FE 設定:

:::note
多くの設定パラメータは `s3_` で始まります。このプレフィックスはすべての Amazon S3 互換ストレージタイプ (例: S3、GCS、MinIO) に使用されます。Azure Blob Storage を使用する場合、プレフィックスは `azure_` です。
:::

#### `run_mode=shared_data`

これにより、共有データの使用が有効になります。

#### `cloud_native_storage_type=S3`

これは S3 互換ストレージまたは Azure Blob Storage が使用されるかどうかを指定します。MinIO では常に S3 です。

### `CREATE storage volume` の詳細

```sql
CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES
    (
         "enabled" = "true",
         "aws.s3.endpoint" = "minio:9000",
         "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
         "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
         "aws.s3.use_instance_profile" = "false",
         "aws.s3.use_aws_sdk_default_behavior" = "false"
     );
```

#### `aws_s3_endpoint=minio:9000`

MinIO エンドポイントとポート番号。

#### `aws_s3_path=starrocks`

バケット名。

#### `aws_s3_access_key=AAAAAAAAAAAAAAAAAAAA`

MinIO アクセスキー。

#### `aws_s3_secret_key=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`

MinIO アクセスキーシークレット。

#### `aws_s3_use_instance_profile=false`

MinIO を使用する場合、アクセスキーが使用されるため、インスタンスプロファイルは MinIO では使用されません。

#### `aws_s3_use_aws_sdk_default_behavior=false`

MinIO を使用する場合、このパラメータは常に false に設定されます。

### FQDN モードの構成

FE を起動するコマンドも変更されます。Docker Compose ファイルの FE サービスコマンドには、オプション `--host_type FQDN` が追加されています。`host_type` を `FQDN` に設定することで、Stream Load ジョブは CN ポッドの完全修飾ドメイン名に転送されます。これは、IP アドレスが Docker 環境に割り当てられた範囲内にあり、通常はホストマシンから利用できないためです。

ホストネットワークと CN 間のトラフィックを許可するための 3 つの変更:

- `--host_type` を `FQDN` に設定
- CN ポート 8040 をホストネットワークに公開
- `starrocks-cn` のエントリをホストファイルに追加し、`127.0.0.1` を指すように設定

---

## まとめ

このチュートリアルでは以下を行いました:

- Docker で StarRocks と MinIO をデプロイ
- MinIO アクセスキーを作成
- MinIO を使用する StarRocks ストレージボリュームを設定
- ニューヨーク市が提供するクラッシュデータと NOAA が提供する気象データをロード
- SQL JOIN を使用して、視界が悪い状態や凍結した道路での運転が悪い考えであることを分析

学ぶべきことはまだあります。Stream Load 中に行われたデータ変換については意図的に詳しく触れませんでした。curl コマンドに関するメモでその詳細を確認できます。

## curl コマンドに関する注意

<Curl />

## より詳しい情報

[StarRocks テーブル設計](../table_design/StarRocks_table_design.md)

[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)

[Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) データセットは、ニューヨーク市によって提供され、これらの [利用規約](https://www.nyc.gov/home/terms-of-use.page) および [プライバシーポリシー](https://www.nyc.gov/home/privacy-policy.page) に従います。

[Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd) (LCD) は、NOAA によって提供され、[免責事項](https://www.noaa.gov/disclaimer) および [プライバシーポリシー](https://www.noaa.gov/protecting-your-privacy) に従います。
