---
displayed_sidebar: docs
---

# Broker のデプロイ

このトピックでは、Broker のデプロイ方法について説明します。Broker を使用すると、StarRocks は HDFS や S3 などのソースからデータを読み取り、独自のコンピューティングリソースでデータを事前処理、ロード、バックアップできます。

## インストーラーのダウンロードと解凍

[ダウンロード](https://www.starrocks.io/download/community)して StarRocks インストーラーを解凍します。

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> **注意**
>
> コマンド内のファイル名を、ダウンロードした実際のファイル名に置き換えてください。

## Broker の設定

**StarRocks-x.x.x/apache_hdfs_broker** に移動します。

```bash
cd StarRocks-x.x.x/apache_hdfs_broker
```

> **注意**
>
> コマンド内のファイル名を、ダウンロードした実際のファイル名に置き換えてください。
Broker の設定ファイル **conf/apache_hdfs_broker.conf** を指定します。デフォルトの設定はそのまま使用できるため、以下の例では設定項目を変更していません。独自の HDFS クラスター設定ファイルをコピーして、**conf** ディレクトリに貼り付けることができます。

## Broker の起動

次のコマンドを実行して Broker を起動します。

```bash
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

## Broker をクラスタに追加

次のコマンドを実行して Broker をクラスタに追加します。

```sql
ALTER SYSTEM ADD BROKER broker_name "broker_ip:broker_port";
```

> **注意**
>
> - デフォルトでは、Broker のポートは `8000` です。
> - 複数の Broker を同時にクラスタに追加する場合、それらは同じ `broker_name` を共有します。

## Broker が起動しているか確認

MySQL クライアントを使用して BE ノードが正しく起動しているか確認できます。

```sql
SHOW PROC "/brokers"\G
```

例:

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G
*************************** 1. row ***************************
          Name: broker1
            IP: x.x.x.x
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
1 row in set (0.00 sec)
```

フィールド `Alive` が `true` の場合、Broker は正しく起動し、クラスタに追加されています。

## Broker の停止

次のコマンドを実行して Broker を停止します。

```bash
./bin/stop_broker.sh --daemon
```

## Broker の設定

Broker の設定項目は、対応する設定ファイル **broker.conf** を変更することでのみ設定できます。変更を有効にするには、Broker を再起動してください。

| 設定項目 | デフォルト | 単位 | 説明 |
| ------------------------- | ------------------ | ------ | ------------------------------------------------------------ |
| hdfs_read_buffer_size_kb | 8192 | KB | HDFS からデータを読み取るために使用されるバッファのサイズ。 |
| hdfs_write_buffer_size_kb | 1024 | KB | HDFS にデータを書き込むために使用されるバッファのサイズ。 |
| client_expire_seconds | 300 | 秒 | 指定された時間後に ping を受信しない場合、クライアントセッションは削除されます。 |
| broker_ipc_port | 8000 | N/A | HDFS の Thrift RPC ポート。 |
| disable_broker_client_expiration_checking | false | N/A | 期限切れの OSS ファイルディスクリプタのチェックとクリアを無効にするかどうか。これにより、場合によっては OSS が閉じるときに Broker がスタックすることがあります。この状況を避けるために、このパラメータを `true` に設定してチェックを無効にすることができます。 |
| sys_log_dir | `${BROKER_HOME}/log` | N/A | システムログ（INFO、WARNING、ERROR、FATAL を含む）を保存するために使用されるディレクトリ。 |
| sys_log_level | INFO | N/A | ログレベル。有効な値には INFO、WARNING、ERROR、FATAL が含まれます。 |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | システムログがログロールに分割されるモード。有効な値には TIME-DAY、TIME-HOUR、SIZE-MB-nnn が含まれます。デフォルト値は、ログが 1 GB ごとにロールに分割されることを示します。 |
| sys_log_roll_num | 30 | N/A | 保持するログロールの数。 |
| audit_log_dir | `${BROKER_HOME}/log` | N/A | 監査ログファイルを保存するディレクトリ。 |
| audit_log_modules | 空文字列 | N/A | StarRocks が監査ログエントリを生成するモジュール。デフォルトでは、StarRocks は slow_query モジュールと query モジュールの監査ログを生成します。複数のモジュールを指定することができ、名前はカンマ（,）とスペースで区切る必要があります。 |
| audit_log_roll_mode | TIME-DAY | N/A | 有効な値には `TIME-DAY`、`TIME-HOUR`、`SIZE-MB-<size>` が含まれます。 |
| audit_log_roll_num | 10 | N/A | audit_log_roll_mode が `SIZE-MB-<size>` に設定されている場合、この設定は機能しません。 |
| sys_log_verbose_modules | com.starrocks | N/A | StarRocks がシステムログを生成するモジュール。有効な値は BE 内の名前空間であり、`starrocks`、`starrocks::vectorized`、`pipeline` を含みます。 |