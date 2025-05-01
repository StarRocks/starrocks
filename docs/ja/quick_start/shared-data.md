---
description: 計算とストレージの分離
displayed_sidebar: docs
---

# ストレージとコンピュートの分離

import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsCompose.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

ストレージとコンピュートを分離するシステムでは、データはAmazon S3、Google Cloud Storage、Azure Blob Storage、MinIOのような他のS3互換ストレージなど、低コストで信頼性の高いリモートストレージシステムに保存されます。ホットデータはローカルにキャッシュされ、キャッシュがヒットすると、クエリパフォーマンスはストレージとコンピュートが結合されたアーキテクチャと同等になります。コンピュートノード (CN) は、必要に応じて数秒で追加または削除できます。このアーキテクチャはストレージコストを削減し、リソースの分離を改善し、弾力性とスケーラビリティを提供します。

このチュートリアルでは以下をカバーします：

- DockerコンテナでのStarRocksの実行
- オブジェクトストレージとしてのMinIOの使用
- StarRocksの共有データ用の設定
- 2つの公開データセットのロード
- SELECTとJOINを使用したデータの分析
- 基本的なデータ変換 (ETLの**T**)

使用されるデータは、NYC OpenDataとNOAAのNational Centers for Environmental Informationによって提供されています。

これらのデータセットは非常に大きいため、このチュートリアルはStarRocksを使用する経験を得ることを目的としており、過去120年間のデータをロードすることはありません。Dockerイメージを実行し、このデータをDockerに4 GBのRAMを割り当てたマシンでロードできます。より大規模でフォールトトレラントなスケーラブルなデプロイメントについては、他のドキュメントを用意しており、後で提供します。

このドキュメントには多くの情報が含まれており、最初にステップバイステップの内容が提示され、最後に技術的な詳細が記載されています。これは以下の目的を順に達成するためです：

1. 読者が共有データデプロイメントでデータをロードし、そのデータを分析できるようにする。
2. 共有データデプロイメントの設定詳細を提供する。
3. ロード中のデータ変換の基本を説明する。

---

## 前提条件

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- Dockerに割り当てられた4 GBのRAM
- Dockerに割り当てられた10 GBの空きディスクスペース

### SQLクライアント

Docker環境で提供されるSQLクライアントを使用するか、システム上のものを使用できます。多くのMySQL互換クライアントが動作し、このガイドではDBeaverとMySQL WorkBenchの設定をカバーしています。

### curl

`curl` はStarRocksにデータロードジョブを発行し、データセットをダウンロードするために使用されます。OSのプロンプトで `curl` または `curl.exe` を実行して、インストールされているか確認してください。curlがインストールされていない場合は、[こちらからcurlを取得してください](https://curl.se/dlwiz/?type=bin)。

### `/etc/hosts`

このガイドで使用されるインジェスト方法はStream Loadです。Stream LoadはFEサービスに接続してインジェストジョブを開始します。FEはその後、ジョブをバックエンドノード、つまりこのガイドではCNに割り当てます。インジェストジョブがCNに接続するためには、CNの名前がオペレーティングシステムに認識されている必要があります。次の行を `/etc/hosts` に追加してください：

```bash
127.0.0.1 starrocks-cn
```

---

## 用語

### FE

フロントエンドノードは、メタデータ管理、クライアント接続管理、クエリプランニング、クエリスケジューリングを担当します。各FEはメモリ内にメタデータの完全なコピーを保存および維持し、FEs間での無差別なサービスを保証します。

### CN

コンピュートノードは、共有データデプロイメントでクエリプランを実行する役割を担っています。

### BE

バックエンドノードは、共有なしデプロイメントでデータストレージとクエリプランの実行の両方を担当します。

:::note
このガイドではBEsを使用しませんが、BEsとCNsの違いを理解するためにこの情報を含めています。
:::

---

## ラボファイルのダウンロード

ダウンロードするファイルは3つあります：

- StarRocksとMinIO環境をデプロイするDocker Composeファイル
- ニューヨーク市のクラッシュデータ
- 気象データ

このガイドでは、GNU Affero General Public Licenseの下で提供されるS3互換のオブジェクトストレージであるMinIOを使用します。

### ラボファイルを保存するディレクトリを作成：

```bash
mkdir quickstart
cd quickstart
```

### Docker Composeファイルのダウンロード

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

### データのダウンロード

次の2つのデータセットをダウンロードします：

#### ニューヨーク市のクラッシュデータ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

#### 気象データ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## StarRocksとMinIOのデプロイ

```bash
docker compose up --detach --wait --wait-timeout 120
```

FE、CN、およびMinIOサービスが正常になるまで約30秒かかります。`quickstart-minio_mc-1` コンテナは `Waiting` のステータスと終了コードを示します。終了コードが `0` の場合は成功を示します。

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

## MinIOのクレデンシャルを確認

StarRocksでMinIOをオブジェクトストレージとして使用するには、StarRocksにMinIOアクセスキーが必要です。アクセスキーはDockerサービスの起動時に生成されました。StarRocksがMinIOに接続する方法をよりよく理解するために、キーが存在することを確認してください。

### MinIOウェブUIを開く

http://localhost:9001/access-keys にアクセスします。ユーザー名とパスワードはDocker composeファイルで指定されており、`miniouser` と `miniopassword` です。1つのアクセスキーがあることが確認できます。キーは `AAAAAAAAAAAAAAAAAAAA` で、MinIOコンソールではシークレットは表示されませんが、Docker composeファイルには `BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB` と記載されています：

![View the MinIO access key](../_assets/quick-start/MinIO-view-key.png)

:::tip
MinIOウェブUIにアクセスキーが表示されない場合は、`minio_mc` サービスのログを確認してください：

```bash
docker compose logs minio_mc
```

`minio_mc` ポッドを再実行してみてください：

```bash
docker compose run minio_mc
```
:::

---

## SQLクライアント

<Clients />

---

## StarRocksの共有データ用設定

この時点でStarRocksが実行され、MinIOも実行されています。MinIOアクセスキーはStarRocksとMinIOを接続するために使用されます。StarRocksが起動したときにMinIOとの接続を確立し、MinIOにデフォルトのストレージボリュームを作成しました。

これはMinIOを使用するデフォルトストレージボリュームを設定するために使用される設定です（これもDocker composeファイルにあります）。設定はこのガイドの最後に詳細に説明されますが、今は `aws_s3_access_key` がMinIOコンソールで見た文字列に設定されていることと、`run_mode` が `shared_data` に設定されていることに注意してください：

```plaintext
#highlight-start
# enable shared data, set storage type, set endpoint
run_mode = shared_data
#highlight-end
cloud_native_storage_type = S3
aws_s3_endpoint = minio:9000

# set the path in MinIO
aws_s3_path = starrocks

#highlight-start
# credentials for MinIO object read/write
aws_s3_access_key = AAAAAAAAAAAAAAAAAAAA
aws_s3_secret_key = BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
#highlight-end
aws_s3_use_instance_profile = false
aws_s3_use_aws_sdk_default_behavior = false

# Set this to false if you do not want default
# storage created in the object storage using
# the details provided above
enable_load_volume_from_conf = true
```

### SQLクライアントでStarRocksに接続

:::tip

`docker-compose.yml` ファイルを含むディレクトリからこのコマンドを実行してください。

mysql CLI以外のクライアントを使用している場合は、今それを開いてください。
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

#### ストレージボリュームを確認

```sql
SHOW STORAGE VOLUMES;
```

```plaintext
+------------------------+
| Storage Volume         |
+------------------------+
| builtin_storage_volume |
+------------------------+
1 row in set (0.00 sec)
```

```sql
DESC STORAGE VOLUME builtin_storage_volume\G
```

:::tip
このドキュメントの一部のSQL、およびStarRocksドキュメントの多くの他のドキュメントは、セミコロンの代わりに `\G` で終わります。`mysql` CLIでは、` \G` はクエリ結果を縦に表示します。

多くのSQLクライアントは縦のフォーマット出力を解釈しないため、`\G` を `;` に置き換える必要があります。
:::

```plaintext
*************************** 1. row ***************************
     Name: builtin_storage_volume
     Type: S3
IsDefault: true
#highlight-start
 Location: s3://starrocks
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"","aws.s3.use_instance_profile":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
#highlight-end
  Enabled: true
  Comment:
1 row in set (0.03 sec)
```

パラメータが設定と一致していることを確認してください。

:::note
フォルダ `builtin_storage_volume` は、バケットにデータが書き込まれるまでMinIOオブジェクトリストに表示されません。
:::

---

## テーブルを作成

<DDL />

---

## 2つのデータセットをロード

StarRocksにデータをロードする方法は多数あります。このチュートリアルでは、最も簡単な方法はcurlとStarRocks Stream Loadを使用することです。

:::tip

データセットをダウンロードしたディレクトリからこれらのcurlコマンドを実行してください。

パスワードを求められます。MySQLの `root` ユーザーにパスワードを割り当てていない場合は、Enterキーを押してください。

:::

`curl` コマンドは複雑に見えますが、チュートリアルの最後で詳細に説明されています。今は、コマンドを実行してデータを分析するためのSQLを実行し、最後にデータロードの詳細を読むことをお勧めします。

### ニューヨーク市の衝突データ - クラッシュ

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

上記のコマンドの出力です。最初のハイライトされたセクションは、期待される結果（OKと1行を除くすべての行が挿入されたこと）を示しています。1行は列数が正しくないためフィルタリングされました。

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

エラーが発生した場合、出力にはエラーメッセージを確認するためのURLが提供されます。エラーメッセージには、Stream Loadジョブが割り当てられたバックエンドノード（`starrocks-cn`）も含まれています。`/etc/hosts` ファイルに `starrocks-cn` のエントリを追加したため、そこに移動してエラーメッセージを読むことができるはずです。

このチュートリアルの開発中に見られた内容の要約を展開します：

<details>

<summary>ブラウザでのエラーメッセージの読み取り</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.

Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### 気象データ

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

## MinIOにデータが保存されていることを確認

MinIOを開き、[http://localhost:9001/browser/starrocks/](http://localhost:9001/browser/starrocks/) にアクセスして、`starrocks/shared/` 以下の各ディレクトリに `data`、`metadata`、`schema` エントリがあることを確認してください。

:::tip
`starrocks/shared/` 以下のフォルダ名は、データをロードするときに生成されます。`shared` の下に1つのディレクトリが表示され、その下にさらに2つのディレクトリが表示されます。それぞれのディレクトリ内に、データ、メタデータ、およびスキーマエントリが見つかります。

![MinIO object browser](../_assets/quick-start/MinIO-data.png)
:::

---

## 質問に答える

<SQL />

---

## StarRocksの共有データ用設定

StarRocksを共有データで使用する経験を積んだ今、設定を理解することが重要です。

### CN設定

ここで使用されるCN設定はデフォルトです。CNは共有データの使用を目的として設計されています。デフォルトの設定は以下の通りです。変更する必要はありません。

```bash
sys_log_level = INFO

# ports for admin, web, heartbeat service
be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
starlet_port = 9070
```

### FE設定

FE設定はデフォルトとは少し異なります。FEはデータがBEノードのローカルディスクではなくオブジェクトストレージに保存されることを期待するように設定する必要があります。

`docker-compose.yml` ファイルは `command` でFE設定を生成します。

```plaintext
# enable shared data, set storage type, set endpoint
run_mode = shared_data
cloud_native_storage_type = S3
aws_s3_endpoint = minio:9000

# set the path in MinIO
aws_s3_path = starrocks

# credentials for MinIO object read/write
aws_s3_access_key = AAAAAAAAAAAAAAAAAAAA
aws_s3_secret_key = BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
aws_s3_use_instance_profile = false
aws_s3_use_aws_sdk_default_behavior = false

# Set this to false if you do not want default
# storage created in the object storage using
# the details provided above
enable_load_volume_from_conf = true
```

:::note
この設定ファイルにはFEのデフォルトエントリは含まれていません。共有データ設定のみが示されています。
:::

デフォルトではないFE設定：

:::note
多くの設定パラメータは `s3_` で始まります。このプレフィックスは、すべてのAmazon S3互換ストレージタイプ（例：S3、GCS、MinIO）に使用されます。Azure Blob Storageを使用する場合、プレフィックスは `azure_` です。
:::

#### `run_mode=shared_data`

これは共有データの使用を有効にします。

#### `cloud_native_storage_type=S3`

これはS3互換ストレージまたはAzure Blob Storageが使用されるかどうかを指定します。MinIOの場合、常にS3です。

#### `aws_s3_endpoint=minio:9000`

ポート番号を含むMinIOエンドポイント。

#### `aws_s3_path=starrocks`

バケット名。

#### `aws_s3_access_key=AA`

MinIOアクセスキー。

#### `aws_s3_secret_key=BB`

MinIOアクセスキーシークレット。

#### `aws_s3_use_instance_profile=false`

MinIOを使用する場合、アクセスキーが使用されるため、インスタンスプロファイルはMinIOでは使用されません。

#### `aws_s3_use_aws_sdk_default_behavior=false`

MinIOを使用する場合、このパラメータは常にfalseに設定されます。

#### `enable_load_volume_from_conf=true`

これがtrueの場合、MinIOオブジェクトストレージを使用して `builtin_storage_volume` という名前のStarRocksストレージボリュームが作成され、作成するテーブルのデフォルトストレージボリュームとして設定されます。

### FQDNモードの設定

FEを起動するコマンドも変更されています。Docker ComposeファイルのFEサービスコマンドには、`--host_type FQDN` オプションが追加されています。`host_type` を `FQDN` に設定することで、Stream LoadジョブがCNポッドの完全修飾ドメイン名に転送されます。これは、IPアドレスがDocker環境に割り当てられた範囲内にあり、通常ホストマシンから利用できないためです。

これら3つの変更により、CNがホストネットワークに転送されることが可能になります：

- `--host_type` を `FQDN` に設定
- CNポート8040をホストネットワークに公開
- `127.0.0.1` を指す `starrocks-cn` のエントリをホストファイルに追加

---

## まとめ

このチュートリアルでは以下を行いました：

- DockerでStarRocksとMinIOをデプロイ
- MinIOアクセスキーを作成
- MinIOを使用するStarRocksストレージボリュームを設定
- ニューヨーク市が提供するクラッシュデータとNOAAが提供する気象データをロード
- SQL JOINを使用して、視界が悪いまたは氷結した道路での運転が悪い考えであることを発見

学ぶべきことはまだあります。Stream Load中に行われたデータ変換については意図的に詳しく説明していません。curlコマンドのメモにその詳細があります。

## curlコマンドのメモ

<Curl />

## さらなる情報

[StarRocksテーブル設計](../table_design/StarRocks_table_design.md)

[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)

[Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) データセットは、ニューヨーク市によってこれらの[利用規約](https://www.nyc.gov/home/terms-of-use.page)および[プライバシーポリシー](https://www.nyc.gov/home/privacy-policy.page)に基づいて提供されています。

[Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd) (LCD) は、NOAAによってこの[免責事項](https://www.noaa.gov/disclaimer)およびこの[プライバシーポリシー](https://www.noaa.gov/protecting-your-privacy)と共に提供されています。
```