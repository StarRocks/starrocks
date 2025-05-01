---
description: 計算とストレージの分離
displayed_sidebar: docs
---

# ストレージとコンピュートの分離

import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsCompose.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

ストレージとコンピュートを分離したシステムでは、データはAmazon S3、Google Cloud Storage、Azure Blob Storage、MinIOのような他のS3互換ストレージといった低コストで信頼性の高いリモートストレージシステムに保存されます。ホットデータはローカルにキャッシュされ、キャッシュがヒットすると、クエリパフォーマンスはストレージとコンピュートが結合されたアーキテクチャと同等になります。コンピュートノード (CN) は、数秒でオンデマンドで追加または削除できます。このアーキテクチャはストレージコストを削減し、リソースの分離を改善し、弾力性とスケーラビリティを提供します。

このチュートリアルでは以下をカバーします：

- DockerコンテナでのStarRocksの実行
- オブジェクトストレージとしてのMinIOの使用
- StarRocksの共有データ用の設定
- 2つの公開データセットのロード
- SELECTとJOINを使用したデータの分析
- 基本的なデータ変換 (ETLの**T**)

使用されるデータはNYC OpenDataとNOAAのNational Centers for Environmental Informationから提供されています。

これらのデータセットは非常に大きいため、このチュートリアルはStarRocksの操作に慣れることを目的としており、過去120年間のデータをロードすることはありません。Dockerイメージを実行し、このデータをDockerに4 GBのRAMを割り当てたマシンでロードできます。より大規模でフォールトトレラントなスケーラブルなデプロイメントについては、他のドキュメントを用意しており、後で提供します。

このドキュメントには多くの情報が含まれており、ステップバイステップの内容が最初に、技術的な詳細が最後に提示されています。これは以下の目的を順に果たすためです：

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

Docker環境で提供されるSQLクライアントを使用するか、システム上のクライアントを使用できます。多くのMySQL互換クライアントが動作し、このガイドではDBeaverとMySQL WorkBenchの設定をカバーしています。

### curl

`curl` はStarRocksへのデータロードジョブの発行とデータセットのダウンロードに使用されます。OSのプロンプトで `curl` または `curl.exe` を実行してインストールされているか確認してください。curlがインストールされていない場合は、[こちらからcurlを入手してください](https://curl.se/dlwiz/?type=bin)。

---

## 用語

### FE

フロントエンドノードはメタデータ管理、クライアント接続管理、クエリプランニング、クエリスケジューリングを担当します。各FEはメモリ内にメタデータの完全なコピーを保存および維持し、FEs間でのサービスの無差別性を保証します。

### CN

コンピュートノードは、共有データデプロイメントでクエリプランを実行する役割を担っています。

### BE

バックエンドノードは、共有なしデプロイメントでデータストレージとクエリプランの実行の両方を担当します。

:::note
このガイドではBEsを使用しませんが、BEsとCNsの違いを理解するためにこの情報を含めています。
:::

---

## StarRocksの起動

オブジェクトストレージを使用して共有データでStarRocksを実行するには、以下が必要です：

- フロントエンドエンジン (FE)
- コンピュートノード (CN)
- オブジェクトストレージ

このガイドでは、GNU Affero General Public Licenseの下で提供されるS3互換のオブジェクトストレージであるMinIOを使用します。

3つの必要なコンテナを提供する環境を用意するために、StarRocksはDocker composeファイルを提供します。

```bash
mkdir quickstart
cd quickstart
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

```bash
docker compose up -d
```

サービスの進行状況を確認します。FEとCNが正常になるまで約30秒かかります。MinIOコンテナは健康状態のインジケータを表示しませんが、MinIOのWeb UIを使用してその健康状態を確認します。

FEとCNが `healthy` のステータスを示すまで `docker compose ps` を実行します：

```bash
docker compose ps
```

```plaintext
SERVICE        CREATED          STATUS                    PORTS
starrocks-cn   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8040->8040/tcp
starrocks-fe   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, 0.0.0.0:9030->9030/tcp
minio          25 seconds ago   Up 24 seconds             0.0.0.0:9000-9001->9000-9001/tcp
```

---

## MinIOのクレデンシャルを生成する

StarRocksでMinIOをオブジェクトストレージとして使用するには、**アクセスキー**を生成する必要があります。

### MinIO Web UIを開く

http://localhost:9001/access-keys にアクセスします。ユーザー名とパスワードはDocker composeファイルで指定されており、`minioadmin` と `minioadmin` です。まだアクセスキーがないことが確認できます。**Create access key +** をクリックします。

MinIOがキーを生成します。**Create** をクリックしてキーをダウンロードします。

![Make sure to click create](../_assets/quick-start/MinIO-create.png)

:::note
アクセスキーは **Create** をクリックするまで保存されません。キーをコピーしてページから離れないでください。
:::

---

## SQLクライアント

<Clients />

---

## データのダウンロード

これらの2つのデータセットをFEコンテナにダウンロードします。

### FEコンテナでシェルを開く

シェルを開き、ダウンロードしたファイル用のディレクトリを作成します：

```bash
docker compose exec starrocks-fe bash
```

```bash
mkdir quickstart
cd quickstart
```

### ニューヨーク市のクラッシュデータ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### 天気データ

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## StarRocksの共有データ用設定

この時点でStarRocksが実行されており、MinIOも実行されています。MinIOアクセスキーはStarRocksとMinIOを接続するために使用されます。

### SQLクライアントでStarRocksに接続する

:::tip

`docker-compose.yml` ファイルを含むディレクトリからこのコマンドを実行します。

mysql CLI以外のクライアントを使用している場合は、今それを開いてください。
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

### ストレージボリュームを作成する

以下の設定の詳細：

- MinIOサーバーはURL `http://minio:9000` で利用可能
- 上記で作成されたバケットは `starrocks` という名前
- このボリュームに書き込まれたデータは、バケット `starrocks` 内の `shared` というフォルダに保存されます
:::tip
フォルダ `shared` は、データがボリュームに初めて書き込まれたときに作成されます
:::
- MinIOサーバーはSSLを使用していません
- MinIOキーとシークレットは `aws.s3.access_key` と `aws.s3.secret_key` として入力されます。以前にMinIO Web UIで作成したアクセスキーを使用します。
- ボリューム `shared` はデフォルトのボリュームです

:::tip
コマンドを実行する前に編集し、MinIOで作成したアクセスキーとシークレットに置き換えてください。
:::

```bash
CREATE STORAGE VOLUME shared
TYPE = S3
LOCATIONS = ("s3://starrocks/shared/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.use_instance_profile" = "false",
    # highlight-start
    "aws.s3.access_key" = "IA2UYcx3Wakpm6sHoFcl",
    "aws.s3.secret_key" = "E33cdRM9MfWpP2FiRpc056Zclg6CntXWa3WPBNMy"
    # highlight-end
);

SET shared AS DEFAULT STORAGE VOLUME;
```

```sql
DESC STORAGE VOLUME shared\G
```

:::tip
このドキュメントの一部のSQLやStarRocksドキュメントの多くのドキュメントでは、セミコロンの代わりに `\G` を使用しています。`\G` はmysql CLIにクエリ結果を縦に表示させます。

多くのSQLクライアントは縦書きのフォーマット出力を解釈しないため、`\G` を `;` に置き換える必要があります。
:::

```plaintext
*************************** 1. row ***************************
     Name: shared
     Type: S3
IsDefault: true
# highlight-start
 Location: s3://starrocks/shared/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"http://minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
# highlight-end
  Enabled: true
  Comment:
1 row in set (0.03 sec)
```

:::note
フォルダ `shared` は、データがバケットに書き込まれるまでMinIOオブジェクトリストに表示されません。
:::

---

## テーブルを作成する

<DDL />

---

## 2つのデータセットをロードする

StarRocksにデータをロードする方法はたくさんあります。このチュートリアルでは、最も簡単な方法としてcurlとStarRocks Stream Loadを使用します。

:::tip

これらのcurlコマンドは、データセットをダウンロードしたディレクトリのFEシェルから実行します。

パスワードを求められます。MySQLの `root` ユーザーにパスワードを割り当てていない場合は、Enterキーを押してください。

:::

`curl` コマンドは複雑に見えますが、チュートリアルの最後で詳細に説明されています。今は、コマンドを実行してデータを分析するためのSQLを実行し、その後にデータロードの詳細を読むことをお勧めします。

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

上記のコマンドの出力です。最初のハイライトされたセクションは、期待される出力 (OKと1行を除くすべての行が挿入されたこと) を示しています。1行は正しい列数を含んでいないためフィルタリングされました。

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
    "ErrorURL": "http://10.5.0.3:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

エラーが発生した場合、出力にはエラーメッセージを確認するためのURLが提供されます。コンテナにはプライベートIPアドレスがあるため、コンテナからcurlを実行して表示する必要があります。

```bash
curl http://10.5.0.3:8040/api/_load_error_log<details from ErrorURL>
```

このチュートリアルの開発中に見られた内容を展開して表示します：

<details>

<summary>ブラウザでエラーメッセージを読む</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.

Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### 天気データ

クラッシュデータをロードしたのと同様に、天気データセットをロードします。

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

## MinIOにデータが保存されていることを確認する

MinIO [http://localhost:9001/browser/starrocks/](http://localhost:9001/browser/starrocks/) を開き、`starrocks/shared/` の各ディレクトリに `data`、`metadata`、`schema` エントリがあることを確認します。

:::tip
`starrocks/shared/` 以下のフォルダ名は、データをロードするときに生成されます。`shared` の下に1つのディレクトリが表示され、その下にさらに2つのディレクトリが表示されるはずです。それぞれのディレクトリ内にデータ、メタデータ、スキーマエントリがあります。

![MinIO object browser](../_assets/quick-start/MinIO-data.png)
:::

---

## 質問に答える

<SQL />

---

## 共有データ用のStarRocks設定

StarRocksを共有データで使用する経験をした今、その設定を理解することが重要です。

### CN設定

ここで使用されるCN設定はデフォルトです。CNは共有データ使用のために設計されています。デフォルトの設定は以下の通りです。変更を加える必要はありません。

```bash
sys_log_level = INFO

be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
```

### FE設定

FE設定はデフォルトとは少し異なります。FEはデータがBEノードのローカルディスクではなくオブジェクトストレージに保存されることを期待するように設定する必要があります。

`docker-compose.yml` ファイルは `command` でFE設定を生成します。

```yml
    command: >
      bash -c "echo run_mode=shared_data >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_meta_port=6090 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_path=starrocks >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_endpoint=minio:9000 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_instance_profile=false >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_storage_type=S3 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_aws_sdk_default_behavior=false >> /opt/starrocks/fe/conf/fe.conf &&
      sh /opt/starrocks/fe/bin/start_fe.sh"
```

これにより、この設定ファイルが生成されます：

```bash title='fe/fe.conf'
LOG_DIR = ${STARROCKS_HOME}/log

DATE = "$(date +%Y%m%d-%H%M%S)"
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:${LOG_DIR}/fe.gc.log.$DATE -XX:+PrintConcurrentLocks"

JAVA_OPTS_FOR_JDK_11="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time"

sys_log_level = INFO

http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
mysql_service_nio_enabled = true

# highlight-start
run_mode=shared_data
aws_s3_path=starrocks
aws_s3_endpoint=minio:9000
aws_s3_use_instance_profile=false
cloud_native_storage_type=S3
aws_s3_use_aws_sdk_default_behavior=false
# highlight-end
```

:::note
この設定ファイルにはデフォルトのエントリと共有データ用の追加が含まれています。共有データ用のエントリはハイライトされています。
:::

デフォルトではないFE設定：

:::note
多くの設定パラメータは `s3_` というプレフィックスが付いています。このプレフィックスはすべてのAmazon S3互換ストレージタイプ（例：S3、GCS、MinIO）に使用されます。Azure Blob Storageを使用する場合、プレフィックスは `azure_` です。
:::

#### `run_mode=shared_data`

これにより共有データの使用が有効になります。

#### `aws_s3_path=starrocks`

バケット名です。

#### `aws_s3_endpoint=minio:9000`

ポート番号を含むMinIOエンドポイントです。

#### `aws_s3_use_instance_profile=false`

MinIOを使用する場合、アクセスキーが使用されるため、MinIOではインスタンスプロファイルは使用されません。

#### `cloud_native_storage_type=S3`

S3互換ストレージまたはAzure Blob Storageのどちらを使用するかを指定します。MinIOの場合、常にS3です。

#### `aws_s3_use_aws_sdk_default_behavior=false`

MinIOを使用する場合、このパラメータは常にfalseに設定されます。

---

## まとめ

このチュートリアルでは：

- DockerでStarRocksとMinioをデプロイしました
- MinIOアクセスキーを作成しました
- MinIOを使用するStarRocksストレージボリュームを設定しました
- ニューヨーク市が提供するクラッシュデータとNOAAが提供する天気データをロードしました
- SQL JOINを使用して、視界が悪い状態や凍結した道路での運転が悪いアイデアであることを分析しました

学ぶべきことはまだあります。Stream Load中に行われたデータ変換については意図的に詳しく触れていません。curlコマンドに関するメモにその詳細があります。

## curlコマンドに関するメモ

<Curl />

## 詳細情報

[StarRocks table design](../table_design/StarRocks_table_design.md)

[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)

[Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) データセットは、ニューヨーク市によってこれらの [利用規約](https://www.nyc.gov/home/terms-of-use.page) および [プライバシーポリシー](https://www.nyc.gov/home/privacy-policy.page) に基づいて提供されています。

[Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd) (LCD) は、NOAAによってこの [免責事項](https://www.noaa.gov/disclaimer) およびこの [プライバシーポリシー](https://www.noaa.gov/protecting-your-privacy) と共に提供されています。
```