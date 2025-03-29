---
displayed_sidebar: docs
sidebar_label: Feature Support
---

# 機能サポート: データロードとアンロード

このドキュメントは、StarRocks がサポートするさまざまなデータロードおよびアンロード方法の機能を概説します。

## ファイル形式

### ロードファイル形式

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th rowspan="2">データソース</th>
        <th colspan="7">ファイル形式</th>
    </tr>
    <tr>
        <th>CSV</th>
        <th>JSON [3]</th>
        <th>Parquet</th>
        <th>ORC</th>
        <th>Avro</th>
        <th>ProtoBuf</th>
        <th>Thrift</th>
    </tr>
    <tr>
        <td>Stream Load</td>
        <td>ローカルファイルシステム、アプリケーション、コネクタ</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td colspan="3">サポート予定</td>
    </tr>
    <tr>
        <td>INSERT from FILES</td>
        <td rowspan="2">HDFS, S3, OSS, Azure, GCS, NFS(NAS) [5]</td>
        <td>Yes (v3.3+)</td>
        <td>サポート予定</td>
        <td>Yes (v3.1+)</td>
        <td>Yes (v3.1+)</td>
        <td colspan="3">サポート予定</td>
    </tr>
    <tr>
        <td>Broker Load</td>
        <td>Yes</td>
        <td>Yes (v3.2.3+)</td>
        <td>Yes</td>
        <td>Yes</td>
        <td colspan="3">サポート予定</td>
    </tr>
    <tr>
        <td>Routine Load</td>
        <td>Kafka</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>Yes (v3.0+) [1]</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>Spark Load</td>
        <td></td>
        <td>Yes</td>
        <td>サポート予定</td>
        <td>Yes</td>
        <td>Yes</td>
        <td colspan="3">サポート予定</td>
    </tr>
    <tr>
        <td>コネクタ</td>
        <td>Flink, Spark</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td colspan="3">サポート予定</td>
    </tr>
    <tr>
        <td>Kafka Connector [2]</td>
        <td>Kafka</td>
        <td colspan="2">Yes (v3.0+)</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td colspan="2">Yes (v3.0+)</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>PIPE [4]</td>
        <td colspan="8">INSERT from FILES と一致</td>
    </tr>
</table>

:::note

[1], [2]\: Schema Registry が必要です。

[3]\: JSON はさまざまな CDC 形式をサポートします。StarRocks がサポートする JSON CDC 形式の詳細については、[JSON CDC format](#json-cdc-formats) を参照してください。

[4]\: 現在、PIPE を使用したロードでは INSERT from FILES のみがサポートされています。

[5]\: 各 BE または CN ノードの同じディレクトリに NAS デバイスを NFS としてマウントし、`file://` プロトコルを介して NFS 内のファイルにアクセスする必要があります。

:::

#### JSON CDC 形式

<table align="center">
    <tr>
        <th></th>
        <th>Stream Load</th>
        <th>Routine Load</th>
        <th>Broker Load</th>
        <th>INSERT from FILES</th>
        <th>Kafka Connector [1]</th>
    </tr>
    <tr>
        <td>Debezium</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>Yes (v3.0+)</td>
    </tr>
    <tr>
        <td>Canal</td>
        <td colspan="5" rowspan="2">サポート予定</td>
    </tr>
    <tr>
        <td>Maxwell</td>
    </tr>
</table>

:::note

[1]\: StarRocks の主キーテーブルに Debezium CDC 形式のデータをロードする際には、`transforms` パラメータを設定する必要があります。

:::

### アンロードファイル形式

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">ターゲット</th>
        <th colspan="4">ファイル形式</th>
    </tr>
    <tr>
        <th>テーブル形式</th>
        <th>リモートストレージ</th>
        <th>CSV</th>
        <th>JSON</th>
        <th>Parquet</th>
        <th>ORC</th>
    </tr>
    <tr>
        <td>INSERT INTO FILES</td>
        <td>N/A</td>
        <td>HDFS, S3, OSS, Azure, GCS, NFS(NAS) [3]</td>
        <td>Yes (v3.3+)</td>
        <td>サポート予定</td>
        <td>Yes (v3.2+)</td>
        <td>Yes (v3.3+)</td>
    </tr>
    <tr>
        <td rowspan="3">INSERT INTO Catalog</td>
        <td>Hive</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes (v3.3+)</td>
        <td>サポート予定</td>
        <td>Yes (v3.2+)</td>
        <td>Yes (v3.3+)</td>
    </tr>
    <tr>
        <td>Iceberg</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>Yes (v3.2+)</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>Hudi/Delta</td>
        <td></td>
        <td colspan="4">サポート予定</td>
    </tr>
    <tr>
        <td>EXPORT</td>
        <td>N/A</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes [1]</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>PIPE</td>
        <td colspan="6">サポート予定 [2]</td>
    </tr>
</table>

:::note

[1]\: Broker プロセスの設定がサポートされています。

[2]\: 現在、PIPE を使用したデータのアンロードはサポートされていません。

[3]\: 各 BE または CN ノードの同じディレクトリに NAS デバイスを NFS としてマウントし、`file://` プロトコルを介して NFS 内のファイルにアクセスする必要があります。

:::

## ファイル形式関連のパラメータ

### ロードファイル形式関連のパラメータ

<table align="center">
    <tr>
        <th rowspan="2">ファイル形式</th>
        <th rowspan="2">パラメータ</th>
        <th colspan="5">ロード方法</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>Broker Load</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td rowspan="6">CSV</td>
        <td>column_separator</td>
        <td>Yes</td>
        <td rowspan="6">Yes (v3.3+)</td>
        <td colspan="3">Yes [1]</td>
    </tr>
    <tr>
        <td>row_delimiter</td>
        <td>Yes</td>
        <td>Yes [2] (v3.1+)</td>
        <td>Yes [3] (v2.2+)</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>enclose</td>
        <td rowspan="4">Yes (v3.0+)</td>
        <td rowspan="4">Yes (v3.0+)</td>
        <td rowspan="2">Yes (v3.0+)</td>
        <td rowspan="4">サポート予定</td>
    </tr>
    <tr>
        <td>escape</td>
    </tr>
    <tr>
        <td>skip_header</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>trim_space</td>
        <td>Yes (v3.0+)</td>
    </tr>
    <tr>
        <td rowspan="4">JSON</td>
        <td>jsonpaths</td>
        <td rowspan="4">Yes</td>
        <td rowspan="4">サポート予定</td>
        <td rowspan="4">Yes (v3.2.3+)</td>
        <td rowspan="3">Yes</td>
        <td rowspan="4">サポート予定</td>
    </tr>
    <tr>
        <td>strip_outer_array</td>
    </tr>
    <tr>
        <td>json_root</td>
    </tr>
    <tr>
        <td>ignore_json_size</td>
        <td>サポート予定</td>
    </tr>
</table>

:::note

[1]\: 対応するパラメータは `COLUMNS TERMINATED BY` です。

[2]\: 対応するパラメータは `ROWS TERMINATED BY` です。

[3]\: 対応するパラメータは `ROWS TERMINATED BY` です。

:::

### アンロードファイル形式関連のパラメータ

<table align="center">
    <tr>
        <th rowspan="2">ファイル形式</th>
        <th rowspan="2">パラメータ</th>
        <th colspan="2">アンロード方法</th>
    </tr>
    <tr>
        <th>INSERT INTO FILES</th>
        <th>EXPORT</th>
    </tr>
    <tr>
        <td rowspan="2">CSV</td>
        <td>column_separator</td>
        <td rowspan="2">Yes (v3.3+)</td>
        <td rowspan="2">Yes</td>
    </tr>
    <tr>
        <td>line_delimiter [1]</td>
    </tr>
</table>

:::note

[1]\: データロードでの対応するパラメータは `row_delimiter` です。

:::

## 圧縮形式

### ロード圧縮形式

<table align="center">
    <tr>
        <th rowspan="2">ファイル形式</th>
        <th rowspan="2">圧縮形式</th>
        <th colspan="5">ロード方法</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>Broker Load</th>
        <th>INSERT from FILES</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td>CSV</td>
        <td rowspan="2">
            <ul>
                <li>defalte</li>
                <li>bzip2</li>
                <li>gzip</li>
                <li>lz4_frame</li>
                <li>zstd</li>
            </ul>
        </td>
        <td>Yes [1]</td>
        <td>Yes [2]</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>JSON</td>
        <td>Yes (v3.2.7+) [3]</td>
        <td>サポート予定</td>
        <td>N/A</td>
        <td>サポート予定</td>
        <td>N/A</td>
    </tr>
    <tr>
        <td>Parquet</td>
        <td rowspan="2">
            <ul>
                <li>gzip</li>
                <li>lz4</li>
                <li>snappy</li>
                <li>zlib</li>
                <li>zstd</li>
            </ul>
        </td>
        <td rowspan="2">N/A</td>
        <td rowspan="2" colspan="2">Yes [4]</td>
        <td rowspan="2">サポート予定</td>
        <td rowspan="2">Yes [4]</td>
    </tr>
    <tr>
        <td>ORC</td>
    </tr>
</table>

:::note

[1]\: 現在、CSV ファイルを Stream Load でロードする場合のみ、`format=gzip` を使用して圧縮形式を指定できます。これは gzip 圧縮された CSV ファイルを示します。`deflate` および `bzip2` 形式もサポートされています。

[2]\: Broker Load は、`format` パラメータを使用して CSV ファイルの圧縮形式を指定することをサポートしていません。Broker Load はファイルの拡張子を使用して圧縮形式を識別します。gzip 圧縮ファイルの拡張子は `.gz` であり、zstd 圧縮ファイルの拡張子は `.zst` です。さらに、`trim_space` や `enclose` などの他の `format` 関連のパラメータもサポートされていません。

[3]\: `compression = gzip` を使用して圧縮形式を指定することがサポートされています。

[4]\: Arrow Library によってサポートされています。`compression` パラメータを設定する必要はありません。

:::

### アンロード圧縮形式

<table align="center">
    <tr>
        <th rowspan="3">ファイル形式</th>
        <th rowspan="3">圧縮形式</th>
        <th colspan="5">アンロード方法</th>
    </tr>
    <tr>
        <th rowspan="2">INSERT INTO FILES</th>
        <th colspan="3">INSERT INTO Catalog</th>
        <th rowspan="2">EXPORT</th>
    </tr>
    <tr>
        <td>Hive</td>
        <td>Iceberg</td>
        <td>Hudi/Delta</td>
    </tr>
    <tr>
        <td>CSV</td>
        <td>
            <ul>
                <li>defalte</li>
                <li>bzip2</li>
                <li>gzip</li>
                <li>lz4_frame</li>
                <li>zstd</li>
            </ul>
        </td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>JSON</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>N/A</td>
    </tr>
    <tr>
        <td>Parquet</td>
        <td rowspan="2">
            <ul>
                <li>gzip</li>
                <li>lz4</li>
                <li>snappy</li>
                <li>zstd</li>
            </ul>
        </td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">サポート予定</td>
        <td rowspan="2">N/A</td>
    </tr>
    <tr>
        <td>ORC</td>
    </tr>
</table>

## 認証情報

### ロード - 認証

<table align="center">
    <tr>
        <th rowspan="2">認証</th>
        <th colspan="5">ロード方法</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>Broker Load</th>
        <th>Routine Load</th>
        <th>External Catalog</th>
    </tr>
    <tr>
        <td>シングル Kerberos</td>
        <td>N/A</td>
        <td>Yes (v3.1+)</td>
        <td>Yes [1] (v2.5 より前のバージョン)</td>
        <td>Yes [2] (v3.1.4+)</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>Kerberos Ticket Granting Ticket (TGT)</td>
        <td>N/A</td>
        <td rowspan="2" colspan="3">サポート予定</td>
        <td rowspan="2">Yes (v3.1.10+/v3.2.1+)</td>
    </tr>
    <tr>
        <td>シングル KDC マルチ Kerberos</td>
        <td>N/A</td>
    </tr>
    <tr>
        <td>基本アクセス認証 (アクセスキー ペア、IAM ロール)</td>
        <td>N/A</td>
        <td colspan="2">Yes (HDFS および S3 互換オブジェクトストレージ)</td>
        <td>Yes [3]</td>
        <td>Yes</td>
    </tr>
</table>

:::note

[1]\: HDFS の場合、StarRocks はシンプル認証と Kerberos 認証の両方をサポートしています。

[2]\: セキュリティプロトコルが `sasl_plaintext` または `sasl_ssl` に設定されている場合、SASL および GSSAPI (Kerberos) 認証の両方がサポートされています。

[3]\: セキュリティプロトコルが `sasl_plaintext` または `sasl_ssl` に設定されている場合、SASL および PLAIN 認証の両方がサポートされています。

:::

### アンロード - 認証

|                 | INSERT INTO FILES  | EXPORT          |
| :-------------- | :----------------: | :-------------: |
| シングル Kerberos | サポート予定    | サポート予定 |

## ロード - その他のパラメータと機能

<table align="center">
    <tr>
        <th rowspan="2">パラメータと機能</th>
        <th colspan="8">ロード方法</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>INSERT from SELECT/VALUES</th>
        <th>Broker Load</th>
        <th>PIPE</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td>部分更新</td>
        <td>Yes (v3.0+)</td>
        <td colspan="2">Yes [1] (v3.3+)</td>
        <td>Yes (v3.0+)</td>
        <td>N/A</td>
        <td>Yes (v3.0+)</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>partial_update_mode</td>
        <td>Yes (v3.1+)</td>
        <td colspan="2">サポート予定</td>
        <td>Yes (v3.1+)</td>
        <td>N/A</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>COLUMNS FROM PATH</td>
        <td>N/A</td>
        <td>Yes (v3.2+)</td>
        <td>N/A</td>
        <td>Yes</td>
        <td>N/A</td>
        <td>N/A</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>timezone または セッション変数 time_zone [2]</td>
        <td>Yes [3]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>サポート予定</td>
        <td>Yes [4]</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>時間精度 - マイクロ秒</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>Yes (v3.1.11+/v3.2.6+)</td>
        <td>サポート予定</td>
        <td>Yes</td>
        <td>Yes</td>
    </tr>
</table>

:::note

[1]\: v3.3 以降、StarRocks は INSERT INTO で列リストを指定することにより、Row モードでの部分更新をサポートしています。

[2]\: パラメータまたはセッション変数でタイムゾーンを設定すると、strftime()、alignment_timestamp()、from_unixtime() などの関数が返す結果に影響します。

[3]\: パラメータ `timezone` のみがサポートされています。

[4]\: セッション変数 `time_zone` のみがサポートされています。

:::

## アンロード - その他のパラメータと機能

<table align="center">
    <tr>
        <th>パラメータと機能</th>
        <th>INSERT INTO FILES</th>
        <th>EXPORT</th>
    </tr>
    <tr>
        <td>target_max_file_size</td>
        <td rowspan="3">Yes (v3.2+)</td>
        <td rowspan="4">サポート予定</td>
    </tr>
    <tr>
        <td>single</td>
    </tr>
    <tr>
        <td>Partitioned_by</td>
    </tr>
    <tr>
        <td>セッション変数 time_zone</td>
        <td>サポート予定</td>
    </tr>
    <tr>
        <td>時間精度 - マイクロ秒</td>
        <td>サポート予定</td>
        <td>サポート予定</td>
    </tr>
</table>